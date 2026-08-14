"""直播轮询器：状态机 + 推送（计划 todo 6）。

live_status 0=未播/1=直播/2=轮播（2 视同未播）；room_id 首次经
``get_live_info(uid).live_room.roomid`` 解析并缓存到 ``live_state.room_id``，
仅 roomid==0/失败时重解析（roomid==0 时跳过 get_room_info、按未播处理）。
首轮静默 seed；0/2→1 推"开播"并重置离线计数/offline_notified（与推送成败
无关）；1→1 且标题变化且 push_title_change 推"改标题"；1→0/2 三连漏判推
"下播"（时长=now-last_live_time，offline_notified 首次尝试即置、失败也置，
每离线期一次、计数不重置；从未观测到 status==1 的 sub 不计数不推）。开播/
下播推送失败记 pending_push（``{"kind", "tries", "timestamp"}``，24h 过期，
先于状态分支按 kind 匹配重投、当轮唯一推送，最多 3 次丢弃告警，相反转移
清空/覆盖）。重启按 DB last_status==1 抑制首次成功轮询的常规推送（静默刷新
last_title/last_live_time，不含 pending）。last_title/last_live_time 仅
status==1 时更新（live_start_time>0 取之，0/2→1 且为 0 回退 now）。错误
独立计数，永不触发下播、永不崩溃。
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

try:
    from ..config import Subscription
    from ..db import Database
    from ..push import format_event_time
    from ..repository import BiliError, BiliRepository
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from push import format_event_time  # type: ignore[import-not-found]
    from repository import BiliError, BiliRepository  # type: ignore[import-not-found]

#: 连续离线轮数阈值，达到才推送"下播"。
_OFFLINE_STRIKES: int = 3
#: pending_push 重投上限（含首次失败），达上限后丢弃并告警。
_MAX_RETRY_ROUNDS: int = 3
#: pending_push 过期时间（秒）。
_PENDING_TTL: int = 24 * 60 * 60

_logger: logging.Logger | None = None


async def _noop_acquire() -> None:
    """默认无操作取牌：未注入令牌桶时行为与之前完全一致。"""
    return None


def _get_logger() -> logging.Logger:
    """返回插件统一 logger；离线环境回退 stdlib logger。"""
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger(__name__)
        else:
            _logger = astrbot_logger
    return _logger


def _now_iso() -> str:
    """当前 UTC 时间的 ISO-8601 字符串（可字典序排序）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


class LivePoller:
    """直播订阅轮询器：检测开播/下播/改标题并推送。

    Args:
        subscription: 规范化后的 live 订阅（id/uid/name 非空）。
        repo: :class:`BiliRepository` 实现（或测试 fake）。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload）。
        send: ``push.send``（subscription, chain, context, status）。
        status: 按 sub_id 的 runtime status dict（可变对象）。
        logger: 显式 logger；缺省用插件统一 logger。
        context: AstrBot Context（或 fake）。
        push_title_change: 标题变化时是否推送"改标题"。
        now: 时钟注入（可调用），测试用可控时钟。
        acquire: 每次 B 站请求前调用的异步取牌函数（调度器注入令牌桶）；
            缺省为无操作，行为不变。
    """

    def __init__(
        self,
        subscription: Subscription,
        repo: BiliRepository,
        db: Database,
        build_chain: Any,
        send: Any,
        status: dict[str, Any],
        logger: logging.Logger | None = None,
        context: Any = None,
        push_title_change: bool = True,
        now: Any = time.time,
        acquire: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.subscription = subscription
        self.repo = repo
        self.db = db
        self.build_chain = build_chain
        self.send = send
        self.status = status
        self._logger = logger if logger is not None else _get_logger()
        self.context = context
        self.push_title_change = push_title_change
        self.now = now
        self._acquire: Callable[[], Awaitable[None]] = (
            acquire if acquire is not None else _noop_acquire
        )
        self.error_count = 0
        self._suppress = False
        self._state_seen = False

    async def poll(self) -> bool:
        """执行一轮直播轮询；返回本轮是否投递了推送。

        ``asyncio.CancelledError`` 透传；仓库/未知异常吞掉记日志并计数。
        """
        try:
            return await self._poll_once()
        except asyncio.CancelledError:
            raise
        except BiliError as exc:
            self._logger.warning(
                "直播轮询失败（sub=%s）: %s", self.subscription.name, exc
            )
            self._record_error(exc, "poll")
            return False
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._logger.error(
                "直播轮询异常（sub=%s）: %s", self.subscription.name, exc, exc_info=True
            )
            self._record_error(exc, "poll")
            return False

    async def _poll_once(self) -> bool:
        sub = self.subscription
        state = await self.db.get_live_state(sub.id)
        if state is None:
            await self._seed(sub)
            return False
        if not self._state_seen:
            self._state_seen = True
            # 重启抑制集：DB 上次状态为直播 → 首次成功轮询观测到 status==1
            # 时静默刷新并抑制常规推送（失败/超时保留）。
            self._suppress = int(state.last_status or 0) == 1
        try:
            status, room = await self._observe(sub, state)
        except BiliError as exc:
            # 错误独立计数，不触发"下播"；room_id 置 0 使下轮重解析。
            self._record_error(exc, "fetch")
            await self.db.upsert_live_state(sub.id, room_id=0)
            return False
        pending = self._decode_pending(state.pending_push)
        if pending is not None:
            return await self._handle_pending(sub, state, pending, status, room)
        return await self._handle_status(sub, state, status, room)

    async def _seed(self, sub: Subscription) -> None:
        """首轮静默 seed：解析 room_id、观测一次、写 live_state，不推送。"""
        try:
            room_id = await self._resolve_room_id(sub.uid)
            if room_id == 0:
                await self.db.upsert_live_state(
                    sub.id,
                    uid=sub.uid,
                    room_id=0,
                    last_status=0,
                    consecutive_offline_count=0,
                    offline_notified=0,
                    last_checked_at=_now_iso(),
                )
                return
            room = await self._fetch_room(room_id)
        except BiliError as exc:
            self._record_error(exc, "seed")
            return
        status = int(room["status"])
        fields: dict[str, Any] = {
            "uid": sub.uid,
            "room_id": room_id,
            "last_status": status,
            "last_title": "",
            "last_live_time": 0,
            "consecutive_offline_count": 0,
            "offline_notified": 0,
            "last_checked_at": _now_iso(),
        }
        if status == 1:
            fields["last_title"] = room["title"]
            start = int(room["live_start_time"])
            fields["last_live_time"] = start if start > 0 else self._now_int()
        await self.db.upsert_live_state(sub.id, **fields)

    async def _observe(
        self, sub: Subscription, state: Any
    ) -> tuple[int, dict[str, Any] | None]:
        """解析 room_id（仅当缓存为 0 时）并拉取一次房间信息。"""
        room_id = int(state.room_id or 0)
        if room_id == 0:
            room_id = await self._resolve_room_id(sub.uid)
            if room_id == 0:
                await self.db.upsert_live_state(sub.id, room_id=0)
                return 0, None  # 从未开播：视同未播，跳过 get_room_info
            await self.db.upsert_live_state(sub.id, room_id=room_id)
        room = await self._fetch_room(room_id)
        return int(room["status"]), room

    async def _handle_pending(
        self,
        sub: Subscription,
        state: Any,
        pending: dict[str, Any],
        status: int,
        room: dict[str, Any] | None,
    ) -> bool:
        """重投失败的 开播/下播：kind 匹配才投，成功/超限/过期/相反转移即清。"""
        kind = pending.get("kind")
        now = self._now_int()
        timestamp = int(pending.get("timestamp", 0))
        if timestamp and now - timestamp > _PENDING_TTL:
            self._logger.info("直播 pending_push 已过期（kind=%s），丢弃", kind)
            await self.db.upsert_live_state(sub.id, pending_push=None)
            return await self._handle_status(sub, state, status, room)
        if kind == "live" and status == 1:
            ok = await self._push("live_on", self._live_payload(room))
        elif kind == "offline" and status in (0, 2):
            ok = await self._push_offline(sub, state)
        else:
            # 相反转移（0/2→1 或 1→0/2）：清空陈旧 pending，走常规分支。
            await self.db.upsert_live_state(sub.id, pending_push=None)
            return await self._handle_status(sub, state, status, room)
        tries = int(pending.get("tries", 0)) + 1
        if ok:
            await self.db.upsert_live_state(sub.id, pending_push=None)
            return True
        if tries >= _MAX_RETRY_ROUNDS:
            await self.db.upsert_live_state(sub.id, pending_push=None)
            self._logger.warning(
                "直播 %s 推送连续 %d 次失败，丢弃重投: sub=%s", kind, tries, sub.name
            )
            return False
        await self.db.upsert_live_state(
            sub.id,
            pending_push=json.dumps(
                {"kind": kind, "tries": tries, "timestamp": timestamp}
            ),
        )
        return False

    async def _handle_status(
        self, sub: Subscription, state: Any, status: int, room: dict[str, Any] | None
    ) -> bool:
        try:
            if status in (0, 2):
                return await self._handle_offline(sub, state, status)
            return await self._handle_live(sub, state, room)
        finally:
            self._suppress = False  # 抑制集在首次成功轮询后清除

    async def _handle_live(
        self, sub: Subscription, state: Any, room: dict[str, Any]
    ) -> bool:
        """status==1：开播 / 改标题 / 重启抑制静默刷新。"""
        title = room["title"]
        start = int(room["live_start_time"])
        prev_status = int(state.last_status or 0)
        prev_title = state.last_title or ""
        now = self._now_int()

        fields: dict[str, Any] = {
            "last_status": 1,
            "last_title": title,
            "consecutive_offline_count": 0,
            "last_checked_at": _now_iso(),
        }
        if start > 0:
            fields["last_live_time"] = start
        elif prev_status in (0, 2):
            fields["last_live_time"] = now  # 0/2→1 且 live_start_time==0 回退 now

        if self._suppress:
            # 重启抑制：静默刷新 last_title/last_live_time，不推送常规内容
            # （pending 先于状态分支处理，不受抑制）。
            await self.db.upsert_live_state(sub.id, **fields)
            return False

        if prev_status in (0, 2):
            # 0/2→1 开播：重置离线计数与 offline_notified（与推送成败无关）。
            fields["offline_notified"] = 0
            await self.db.upsert_live_state(sub.id, **fields)
            ok = await self._push("live_on", self._live_payload(room))
            if not ok:
                await self.db.upsert_live_state(
                    sub.id,
                    pending_push=json.dumps(
                        {"kind": "live", "tries": 1, "timestamp": now}
                    ),
                )
            return ok

        # 1→1
        await self.db.upsert_live_state(sub.id, **fields)
        if title != prev_title and self.push_title_change:
            return await self._push(
                "live_title",
                {
                    "name": sub.name,
                    "old_title": prev_title,
                    "new_title": title,
                    "event_time": format_event_time(self._now_int()),
                    "url": self._room_url(int(state.room_id or 0)),
                },
            )
        return False

    async def _handle_offline(self, sub: Subscription, state: Any, status: int) -> bool:
        """status in (0,2)：3 连漏判后推一次下播，计数不重置。"""
        now = self._now_int()
        armed = (
            int(state.last_status or 0) == 1
            or int(state.consecutive_offline_count or 0) > 0
        )
        count = int(state.consecutive_offline_count or 0) + 1
        fields: dict[str, Any] = {
            "last_status": status,
            "consecutive_offline_count": count,
            "last_checked_at": _now_iso(),
        }
        if not armed:
            fields["consecutive_offline_count"] = 0  # 从未直播：不计数不推
            await self.db.upsert_live_state(sub.id, **fields)
            return False
        if count < _OFFLINE_STRIKES or state.offline_notified:
            await self.db.upsert_live_state(sub.id, **fields)
            return False
        # 第 3 次且未通知：推下播；首次尝试即置 offline_notified（失败也置）。
        ok = await self._push_offline(sub, state)
        fields["offline_notified"] = 1
        fields["live_ended_at"] = now
        if not ok:
            fields["pending_push"] = json.dumps(
                {"kind": "offline", "tries": 1, "timestamp": now}
            )
        await self.db.upsert_live_state(sub.id, **fields)
        return ok

    # -- 推送 -------------------------------------------------------------

    def _live_payload(self, room: dict[str, Any]) -> dict[str, Any]:
        return {
            "name": self.subscription.name,
            "title": room["title"],
            "area_name": room["area_name"],
            "live_start_time": format_event_time(room["live_start_time"]),
            "url": self._room_url(int(room["room_id"])),
            "cover": room["cover"],
        }

    async def _push_offline(self, sub: Subscription, state: Any) -> bool:
        now = self._now_int()
        duration = max(0, now - int(state.last_live_time or now))
        return await self._push(
            "live_off",
            {
                "name": sub.name,
                "duration": duration,
                "event_time": format_event_time(now),
                "url": self._room_url(int(state.room_id or 0)),
            },
        )

    async def _push(self, event_type: str, payload: dict[str, Any]) -> bool:
        chain = self.build_chain(event_type, payload)
        results = await self.send(self.subscription, chain, self.context, self.status)
        return any(results.values())

    # -- 仓库 / 数据层 -----------------------------------------------------

    async def _resolve_room_id(self, uid: int) -> int:
        await self._acquire()
        info = await self.repo.get_live_info(uid)
        try:
            return int(info["live_room"]["roomid"])
        except (KeyError, TypeError, ValueError):
            return 0

    async def _fetch_room(self, room_id: int) -> dict[str, Any]:
        await self._acquire()
        info = await self.repo.get_room_info(room_id)
        if isinstance(info.get("room_info"), dict):  # SDK 嵌套 room_info 形状
            info = info["room_info"]
        return {
            "room_id": room_id,
            "status": int(info.get("live_status", 0)),
            "title": str(info.get("title", "")),
            "area_name": str(info.get("area_name", "")),
            "live_start_time": int(info.get("live_start_time", 0)),
            "cover": str(info.get("cover", "")),
        }

    @staticmethod
    def _decode_pending(raw: str | None) -> dict[str, Any] | None:
        if not raw:
            return None
        try:
            pending = json.loads(raw)
        except (TypeError, ValueError):
            return None
        return pending if isinstance(pending, dict) else None

    @staticmethod
    def _room_url(room_id: int) -> str:
        return f"https://live.bilibili.com/{room_id}"

    def _now_int(self) -> int:
        return int(self.now())

    def _record_error(self, exc: Exception, where: str) -> None:
        self.error_count += 1
        entry = self.status.get(self.subscription.id)
        if entry is None:
            entry = SimpleNamespace(last_push_at=None, last_error=None)
            self.status[self.subscription.id] = entry
        entry.last_error = f"{where}: {exc}"
