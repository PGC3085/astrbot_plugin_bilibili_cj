"""动态轮询器：静默 seed + 空 offset 扫描推送新动态（计划 todo 7）。

- 每轮从空 offset（最新在前）拉取，翻页直到 ``has_more`` 为假或触
  :data:`_MAX_PAGES` 页上限（防失控；触上限记告警）。
- 首轮静默 seed：全部 ``dynamic_id`` 写入 ``known_dynamics``（不推送），
  seed 标志持久化到 ``dynamic_state.seeded``——重启恢复，避免重启后重 seed
  吞掉停机期新动态；即使首轮触页上限也照常置位。
- seed 后每轮只推送新项，按 ``(sub_id, dynamic_id)`` 经
  ``insert_dynamic_if_new`` 去重（PK 幂等吸收重复）。
- mark-after-send：任一 session 成功即视为已见；全部失败由
  ``retry_counts[sub_id][dynamic_id]``（main.py 持有、跨重建保留）计数重试，
  达 :data:`_MAX_RETRY_ROUNDS` 轮上限后仍标记并告警（行保留即"仍标记"）。
- 消息构造在 :mod:`poller.dynamic_parser` 中：DDBOT ``news.tmpl`` 同款动作
  句式/类型专属行/转发标注行，兼容新 polymer API 与旧 API，并正确处理
  ``itemOpusStyle`` 的 ``DYNAMIC_TYPE_DRAW`` + ``MAJOR_TYPE_OPUS`` 图文动态
  （正文与多图不再丢失）。
- 仓库/未知异常吞掉记日志；``asyncio.CancelledError`` 透传。

``build_chain`` / ``send`` 由 main.py 注入 push 模块实现（离线测试可替换）。
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from types import SimpleNamespace
from typing import Any

try:
    from . import dynamic_parser
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from poller import dynamic_parser  # type: ignore[import-not-found]

try:
    from .. import util
    from ..config import Subscription
    from ..db import Database
    from ..repository import BiliError, BiliRepository
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    import util  # type: ignore[import-not-found]
    from config import Subscription  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from repository import BiliError, BiliRepository  # type: ignore[import-not-found]

#: 单轮扫描的页数上限（超出即停止翻页并告警）。
_MAX_PAGES: int = 10
#: 推送全失败后的最大重试轮数，达上限后仍标记为已见并告警。
_MAX_RETRY_ROUNDS: int = 3
#: seed 标志所在持久化表（v2 代标记：解析缺陷修复后强制重 seed，避免洪水推送）。
_SEED_TABLE: str = "dynamic_state_v2"

#: 兼容旧导入路径（tests 与自查脚本从 poller.dynamic 导入常量）。
TYPE_ACTION: dict[int, str] = dynamic_parser.TYPE_ACTION
FORWARD_ACTION: dict[int, str] = dynamic_parser.FORWARD_ACTION


class DynamicPoller:
    """动态订阅轮询器：检测并推送 UP 主新动态。

    Args:
        subscription: 规范化后的 dynamic 订阅（uid 非空）。
        repo: :class:`BiliRepository` 实现（或测试 fake）。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload -> str|MessageChain）。
        send: ``push.send``（subscription, chain, context, status -> dict[str, bool]）。
        context: AstrBot Context（或暴露 ``async send_message(session, chain) -> bool``
            的 fake）。
        status: main.py 持有的 runtime status dict（按 sub_id，可变对象）。
        retry_counts: main.py 持有的重试计数 dict（``{sub_id: {dynamic_id: n}}``），
            跨 poller 重建保留；``initialize()`` 清空即重启后重新计数。
        logger: 显式 logger；缺省用插件统一 logger。
        acquire: 每轮轮询开始前调用的异步取牌函数（调度器注入令牌桶，
            per-poll 限速）；缺省为无操作，行为不变。
        push_cover: 是否在推送中携带图片（``poll.push_dynamic_cover``）；
            部分平台（如飞书）图文混合消息存在兼容问题时关闭以仅推送文字。
        push_live_share: 是否推送「直播分享」类动态（``poll.push_dynamic_live_share``，
            类型码 4308）。B 站会在直播结束后自动生成该类动态（非 UP 主动发送），
            缺省不推送以避免与开播/下播通知重复。
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
        logger: Any | None = None,
        acquire: Callable[[], Awaitable[None]] | None = None,
        push_cover: bool = True,
        push_live_share: bool = False,
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
            acquire if acquire is not None else util.noop_acquire
        )
        self._logger = logger if logger is not None else util.get_logger(__name__)
        self.push_cover = push_cover
        self.push_live_share = push_live_share
        self.error_count = 0

    async def poll(self) -> None:
        """执行一轮动态扫描；仓库/未知异常记录错误状态后吞掉，不向上抛出。

        轮询开始前取一枚令牌（per-poll 限速：每轮只取一枚，轮内分页请求不限速）。
        失败会写 ``status.last_error`` 并递增 ``error_count``（调度器据此退避
        与自动禁用）。``asyncio.CancelledError`` 透传（任务取消属正常 shutdown）。
        """
        try:
            await self._acquire()
            await self._poll_once()
        except asyncio.CancelledError:
            raise
        except BiliError as exc:
            self._record_error(exc, "动态轮询")
            self._logger.warning(
                "动态轮询失败（sub=%s）: %s", self.subscription.name, exc
            )
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._record_error(exc, "动态轮询")
            self._logger.error(
                "动态轮询异常（sub=%s）: %s",
                self.subscription.name,
                exc,
                exc_info=True,
            )

    def _record_error(self, exc: Exception, where: str) -> None:
        """记录轮询错误：写 ``status[sub_id].last_error`` 并递增 ``error_count``。

        与 LivePoller 同款信号：调度器据此累计连续失败、指数退避并自动禁用；
        缺失修复前动态/合集错误对调度器完全不可见。
        """
        self.error_count += 1
        entry = self.status.get(self.subscription.id)
        if entry is None:
            entry = SimpleNamespace(last_push_at=None, last_error=None)
            self.status[self.subscription.id] = entry
        entry.last_error = f"{where}: {exc}"

    async def _poll_once(self) -> None:
        sub = self.subscription
        if sub.uid is None:
            self._logger.warning("动态订阅缺少 uid，跳过: %s", sub.name)
            return
        items = await self._fetch_feed(sub.uid)
        if not await self.db.get_seeded(_SEED_TABLE, sub.id):
            await self._seed(sub, items)
            return
        for item in items:
            await self._handle_item(sub, item)

    async def _fetch_feed(self, uid: int) -> list[dict[str, Any]]:
        """从空 offset 拉取动态，翻页直到 ``has_more`` 为假或达页数上限。"""
        collected: list[dict[str, Any]] = []
        offset: str | int = 0
        pages = 0
        while True:
            resp = await self.repo.get_dynamics(uid, offset=offset)
            pages += 1
            items = resp.get("items")
            if isinstance(items, list):
                collected.extend(items)
            if not resp.get("has_more"):
                break
            if pages >= _MAX_PAGES:
                self._logger.warning(
                    "动态扫描已达 %d 页上限（has_more 仍为真），停止翻页: sub=%s",
                    _MAX_PAGES,
                    self.subscription.name,
                )
                break
            offset = resp.get("offset", offset)
        return collected

    async def _seed(self, sub: Subscription, items: list[dict[str, Any]]) -> None:
        """首轮静默 seed：全量写入 known_dynamics（不推送），持久化 seed 标志。"""
        for item in items:
            dyn_id = self._dynamic_id(item)
            if dyn_id:
                await self.db.insert_dynamic_if_new(
                    sub.id, dyn_id, self._dynamic_type(item)
                )
        await self.db.set_seeded(_SEED_TABLE, sub.id, True)

    async def _handle_item(self, sub: Subscription, item: dict[str, Any]) -> None:
        """处理单条动态：去重 → 推送 → 按结果维护 mark/重试计数。

        mark-after-send：``insert_dynamic_if_new`` 即持久化 mark；推送全失败时
        保留重试计数继续重试（达上限后告警并停止，行保留即"仍标记"）；
        任一 session 成功则清除重试计数。
        """
        dyn_id = self._dynamic_id(item)
        if not dyn_id:
            return
        type_ = self._dynamic_type(item)
        if type_ == 4308 and not self.push_live_share:
            # 直播分享动态（B 站自动生成，非 UP 主动发送）：默认不推送，
            # 也不写入去重记录（后续轮次保持可被配置重新启用时捕获）。
            return
        retries = self.retry_counts.get(sub.id, {}).get(dyn_id, 0)
        newly = await self.db.insert_dynamic_if_new(sub.id, dyn_id, type_)
        if not newly and retries == 0:
            return  # 已见且无进行中的重试
        chain = self.build_chain("dynamic", self._payload(sub, item, dyn_id, type_))
        results = await self.send(sub, chain, self.context, self.status)
        if any(results.values()):
            if retries:
                self.retry_counts[sub.id].pop(dyn_id, None)
            return
        retries += 1
        self.retry_counts.setdefault(sub.id, {})[dyn_id] = retries
        if retries >= _MAX_RETRY_ROUNDS:
            self.retry_counts[sub.id].pop(dyn_id, None)
            self._logger.warning(
                "动态推送连续 %d 轮失败，已标记为已见不再重试: sub=%s dynamic=%s",
                _MAX_RETRY_ROUNDS,
                sub.name,
                dyn_id,
            )

    # ------------------------------------------------------------------
    # 条目解析与载荷构造（全部委托给 dynamic_parser，本类只关心调度语义）
    # ------------------------------------------------------------------

    @staticmethod
    def _dynamic_id(item: dict[str, Any]) -> str:
        """提取动态 ID（新 polymer ``id_str`` 优先，旧 API 兜底）。"""
        return dynamic_parser.extract_id(item)

    @staticmethod
    def _dynamic_type(item: dict[str, Any]) -> int:
        """解析内部类型码（含 ``DYNAMIC_TYPE_DRAW`` + ``MAJOR_TYPE_OPUS`` 裁决）。"""
        return dynamic_parser.extract_type(item)

    def _payload(
        self, sub: Subscription, item: dict[str, Any], dyn_id: str, type_: int
    ) -> dict[str, Any]:
        """构造 dynamic 推送载荷（DDBOT ``news.tmpl`` 同款，见 dynamic_parser）。"""
        return dynamic_parser.build_payload(
            sub,
            item,
            dyn_id,
            type_,
            push_cover=self.push_cover,
        )
