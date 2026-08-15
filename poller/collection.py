"""合集轮询器：静默 seed + 逐页全量扫描推送未知 bvid（计划 todo 8）。

职责：

- 首轮静默 seed：从 ``pn=1`` 全量翻页（``ps=20``；合集有限，翻到
  ``len(page) < ps`` 或空页即止，无页数上限），把每个 bvid 写入
  ``known_videos``（静默标记，不推送），并把 seed 标志持久化到
  ``collection_state.seeded``——重启恢复，避免空合集每轮重 seed 吞掉
  新视频、或重启吞掉停机期新视频。
- seed 后每轮同样从 ``pn=1`` 全量扫描，逐页处理**所有**未知 bvid（不因
  "遇到首个已见视频" 提前停止——升序/自定义排序下会漏掉新增），按
  ``(sub_id, bvid)`` 经 ``insert_video_if_new`` 去重。
- mark-after-send：任一 session 推送成功即视为已见；全部失败则不确认，
  由 ``retry_counts[sub_id][bvid]``（main.py 持有、跨重建保留）计数重试，
  达 :data:`_MAX_RETRY_ROUNDS` 轮上限后仍标记并告警。PK 幂等吸收重复。
- 仓库异常（:class:`BiliError`）与未知异常一律吞掉记日志，不向上抛出；
  ``asyncio.CancelledError`` 透传以保证轮询任务可被干净取消。

``build_chain`` / ``send`` 由 main.py 注入 push 模块实现（离线测试可替换）。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
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

#: 每页拉取条数（计划规定 ps=20）。
_PAGE_SIZE: int = 20
#: 推送全失败后的最大重试轮数，达上限后仍标记为已见并告警。
_MAX_RETRY_ROUNDS: int = 3
#: seed 标志所在持久化表（v2 代标记：解析缺陷修复后强制重 seed，避免洪水推送）。
_SEED_TABLE: str = "collection_state_v2"

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


class CollectionPoller:
    """合集订阅轮询器：检测并推送合集新增视频。

    Args:
        subscription: 规范化后的 collection 订阅（uid/list_id/series_type 非空）。
        repo: :class:`BiliRepository` 实现（或测试 fake）。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload -> str|MessageChain）。
        send: ``push.send``（subscription, chain, context, status -> dict[str, bool]）。
        context: AstrBot Context（或暴露 ``async send_message(session, chain) -> bool``
            的 fake）。
        status: main.py 持有的 runtime status dict（按 sub_id，可变对象）。
        retry_counts: main.py 持有的重试计数 dict（``{sub_id: {bvid: n}}``），
            跨 poller 重建保留；``initialize()`` 清空即重启后重新计数。
        logger: 显式 logger；缺省用插件统一 logger。
        acquire: 每轮轮询开始前调用的异步取牌函数（调度器注入令牌桶，
            per-poll 限速）；缺省为无操作，行为不变。
        push_cover: 是否在推送中携带封面图片（``poll.push_collection_cover``）；
            部分平台（如飞书）图文混合消息存在兼容问题时关闭以仅推送文字。
    """

    def __init__(
        self,
        subscription: Subscription,
        repo: BiliRepository,
        db: Database,
        build_chain: Callable[[str, dict[str, Any]], Any],
        send: Callable[
            [Subscription, Any, Any, dict[str, Any]], Awaitable[dict[str, bool]]
        ],
        context: Any,
        status: dict[str, Any],
        retry_counts: dict[str, dict[str, int]],
        logger: logging.Logger | None = None,
        acquire: Callable[[], Awaitable[None]] | None = None,
        push_cover: bool = True,
    ) -> None:
        self.subscription = subscription
        self.repo = repo
        self.db = db
        self.build_chain = build_chain
        self.send = send
        self.context = context
        self.status = status
        self.retry_counts = retry_counts
        self._acquire: Callable[[], Awaitable[None]] = (
            acquire if acquire is not None else _noop_acquire
        )
        self.push_cover = push_cover
        self._logger = logger if logger is not None else _get_logger()

    async def poll(self) -> None:
        """执行一轮合集扫描；仓库/未知异常吞掉记日志，不向上抛出。

        轮询开始前取一枚令牌（per-poll 限速：每轮只取一枚，轮内分页请求不限速）。
        ``asyncio.CancelledError`` 透传（任务取消属正常 shutdown 路径）。
        """
        try:
            await self._acquire()
            await self._poll_once()
        except asyncio.CancelledError:
            raise
        except BiliError as exc:
            self._logger.warning(
                "合集轮询失败（sub=%s, list_id=%s）: %s",
                self.subscription.name,
                self.subscription.list_id,
                exc,
            )
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._logger.error(
                "合集轮询异常（sub=%s）: %s",
                self.subscription.name,
                exc,
                exc_info=True,
            )

    async def _poll_once(self) -> None:
        sub = self.subscription
        if sub.uid is None or sub.list_id is None or sub.series_type is None:
            self._logger.warning(
                "合集订阅缺少 uid/list_id/series_type，跳过: %s", sub.name
            )
            return
        pages = await self._fetch_all_pages(sub)
        if not await self.db.get_seeded(_SEED_TABLE, sub.id):
            await self._seed(sub, pages)
            return
        for list_name, items in pages:
            for item in items:
                await self._handle_item(sub, list_name, item)

    async def _fetch_all_pages(
        self, sub: Subscription
    ) -> list[tuple[str, list[dict[str, Any]]]]:
        """从 pn=1 全量翻页拉取（ps=20），返回 ``(list_name, items)`` 列表。

        合集有限：``len(items) < ps``、空页或响应缺 ``archives`` 即停止。
        """
        pages: list[tuple[str, list[dict[str, Any]]]] = []
        pn = 1
        while True:
            resp = await self.repo.get_videos(
                sub.uid, sub.list_id, sub.series_type, pn, _PAGE_SIZE
            )
            list_name = self._resolve_list_name(resp, sub)
            archives = resp.get("archives")
            if not isinstance(archives, list):
                pages.append((list_name, []))
                break
            pages.append((list_name, archives))
            if len(archives) < _PAGE_SIZE:
                break
            pn += 1
        return pages

    @staticmethod
    def _resolve_list_name(resp: dict[str, Any], sub: Subscription) -> str:
        """从响应 ``meta.name`` 解析合集名；缺失时回退订阅名。"""
        meta = resp.get("meta")
        name = meta.get("name") if isinstance(meta, dict) else None
        return str(name) if name else sub.name

    async def _seed(
        self, sub: Subscription, pages: list[tuple[str, list[dict[str, Any]]]]
    ) -> None:
        """首轮静默 seed：全量写入 known_videos（不推送），持久化 seed 标志。"""
        for _list_name, items in pages:
            for item in items:
                bvid = item.get("bvid")
                if bvid:
                    await self.db.insert_video_if_new(
                        sub.id, bvid, sub.uid, sub.list_id
                    )
        await self.db.set_seeded(_SEED_TABLE, sub.id, True)

    async def _handle_item(
        self, sub: Subscription, list_name: str, item: dict[str, Any]
    ) -> None:
        """处理单条 archive：去重 → 推送 → 按结果维护 mark/重试计数。

        mark-after-send：``insert_video_if_new`` 即持久化 mark；推送全失败时
        保留重试计数继续重试（达上限后告警并停止，行保留即"仍标记"）；
        任一 session 成功则清除重试计数。
        """
        bvid = item.get("bvid")
        if not bvid:
            return
        retries = self.retry_counts.get(sub.id, {}).get(bvid, 0)
        newly = await self.db.insert_video_if_new(sub.id, bvid, sub.uid, sub.list_id)
        if not newly and retries == 0:
            return  # 已见且无进行中的重试
        chain = self.build_chain("collection", self._payload(sub, list_name, item))
        results = await self.send(sub, chain, self.context, self.status)
        if any(results.values()):
            if retries:
                self.retry_counts[sub.id].pop(bvid, None)
            return
        retries += 1
        self.retry_counts.setdefault(sub.id, {})[bvid] = retries
        if retries >= _MAX_RETRY_ROUNDS:
            self.retry_counts[sub.id].pop(bvid, None)
            self._logger.warning(
                "合集视频推送连续 %d 轮失败，已标记为已见不再重试: sub=%s bvid=%s",
                _MAX_RETRY_ROUNDS,
                sub.name,
                bvid,
            )

    def _payload(
        self, sub: Subscription, list_name: str, item: dict[str, Any]
    ) -> dict[str, Any]:
        """构造 collection 推送载荷（缺失键防御；封面按 push_cover 开关携带）。

        封面位于载荷的 ``cover`` 字段，由 ``push.build_chain`` 追加到消息链
        **尾部**（文字在前），规避部分平台图文顺序兼容问题。
        """
        bvid = item.get("bvid", "")
        cover = item.get("pic")
        payload: dict[str, Any] = {
            "name": sub.name,
            "video_title": str(item.get("title", "")),
            "list_name": list_name,
            "publish_time": format_event_time(
                item.get("pubdate", item.get("pub_time"))
            ),
            "url": f"https://www.bilibili.com/video/{bvid}",
        }
        if self.push_cover and cover:
            payload["cover"] = str(cover)
        return payload
