"""调度器：任务编排 + 限速 + 退避 + 自动禁用 + 配置热感知（计划 todo 10）。

职责：

- :class:`TokenBucket`：异步令牌桶（容量 3、速率=各启用订阅聚合轮询需求，
  不低于 ``1/global_min_interval_sec``），由 scheduler 构建并注入各 poller；
  poller 在**每轮轮询开始时**阻塞取一枚令牌——全局轮询频率上限，保证各订阅
  实际间隔贴近配置值（不因桶速率过低而被拖慢）；SdkRepository 不持有桶。
  时间驱动（``now`` 可注入时钟），等待时长经 ``sleep`` 注入。
- :class:`Scheduler`：**每个订阅一个独立轮询任务**（不再按类型串行），各订阅
  按 ``max(poll_interval_sec, global_min_interval_sec) + uniform(0, jitter)``
  独立间隔执行，互不拖慢；连续失败指数退避（底数 2、上限 5min）；连续失败
  N=10 自动禁用（仅运行时，重启恢复）并向其全部会话推送告警；被禁用订阅
  按自身间隔重查状态。
- 错误信号：轮询前把 ``status[sub_id].last_error`` 置 None，轮询后非 None 即
  本轮出错（轮询器/推送失败都会写 last_error）；另以 ``poller.error_count``
  增量兜底（LivePoller 持有该属性）。dynamic/collection 的轮询错误同样经
  last_error 可检测（本设计下所有类型的轮询错误都会写 last_error）。
- 生命周期：``start()`` / ``stop()`` / ``rebuild()``（重建桶与 poller、
  status/retry_counts/auto-disable 保留）/ ``clear_disabled()``；
  ``maintenance_loop()`` 每 6h 调 ``db.prune_old()``，
  ``create_maintenance_task()`` 供 main.py 取独立稳定引用（不随重建取消）。

调度器循环内不自行重建：配置变更统一走 main.py 的 ``request_rebuild``（todo 13）。
"""

from __future__ import annotations

import asyncio
import logging
import random
import time
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, Awaitable, Callable

try:
    from .config import Subscription, coerce_bool
    from .db import Database
    from .poller.collection import CollectionPoller
    from .poller.dynamic import DynamicPoller
    from .poller.live import LivePoller
    from .repository import BiliRepository, SdkRepository
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription, coerce_bool  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from poller.collection import CollectionPoller  # type: ignore[import-not-found]
    from poller.dynamic import DynamicPoller  # type: ignore[import-not-found]
    from poller.live import LivePoller  # type: ignore[import-not-found]
    from repository import BiliRepository, SdkRepository  # type: ignore[import-not-found]

#: 令牌桶容量（允许的瞬时并发轮询数，随后按速率匀速补充）。
_BUCKET_CAPACITY: int = 3
#: 错误退避指数底数。
_BACKOFF_BASE: float = 2.0
#: 错误退避上限（秒，5 分钟）。
_BACKOFF_MAX_SEC: float = 300.0
#: 连续失败 N 次后自动禁用订阅。
_MAX_CONSECUTIVE_ERRORS: int = 10
#: 维护任务周期（秒，6 小时）。
_MAINTENANCE_INTERVAL_SEC: float = 6 * 3600.0

_logger: logging.Logger | None = None


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


class TokenBucket:
    """异步令牌桶：全局请求速率上限。

    桶容量为最大瞬时令牌数；令牌按 ``rate`` 个/秒基于流逝时间补充
    （``now`` 可注入时钟）；``acquire`` 无令牌时按缺口时长睡眠等待
    （``sleep`` 可注入，测试用假时钟/假 sleep 全确定性）。

    Args:
        capacity: 桶容量（≥1）。
        rate: 令牌补充速率（个/秒，>0）。
        now: 时钟注入（可调用，返回秒），默认 ``time.monotonic``。
        sleep: 等待注入（可调用），默认 ``asyncio.sleep``。
    """

    def __init__(
        self,
        capacity: int,
        rate: float,
        now: Callable[[], float] | None = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        if capacity < 1:
            raise ValueError("capacity must be >= 1")
        if rate <= 0:
            raise ValueError("rate must be > 0")
        self._capacity = float(capacity)
        self._rate = float(rate)
        self._now: Callable[[], float] = now if now is not None else time.monotonic
        self._sleep: Callable[[float], Awaitable[None]] = (
            sleep if sleep is not None else asyncio.sleep
        )
        self._tokens = float(capacity)
        self._last_refill = self._now()
        self._lock = asyncio.Lock()

    def _refill(self) -> None:
        """按流逝时间补充令牌（不超过容量）。"""
        current = self._now()
        elapsed = current - self._last_refill
        if elapsed > 0:
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = current

    async def acquire(self) -> None:
        """阻塞直到取到一枚令牌（可被取消，取消安全）。"""
        while True:
            async with self._lock:
                self._refill()
                if self._tokens >= 1.0:
                    self._tokens -= 1.0
                    return
                wait = (1.0 - self._tokens) / self._rate
            await self._sleep(wait)

    async def try_acquire_nowait(self) -> bool:
        """非阻塞取牌：有令牌立即取走并返回 True，否则返回 False。"""
        async with self._lock:
            self._refill()
            if self._tokens >= 1.0:
                self._tokens -= 1.0
                return True
            return False


class Scheduler:
    """三类订阅轮询任务编排：限速 + 退避 + 自动禁用 + 热重建。

    Args:
        subscriptions: 规范化后的订阅列表（初始快照，重建时整体替换）。
        credential_cfg: B 站凭据配置字典；``repo`` 为 None 时据此构建
            ``SdkRepository``（重建时以最新凭据重建）。
        repo: :class:`BiliRepository` 实例（测试注入 fake）；None 时由
            ``credential_cfg`` 构建并负责重建。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload）。
        send: ``push.send``（subscription, chain, context, status）。
        context: AstrBot Context（或 fake）。
        status: 按 sub_id 的 runtime status dict（main.py 持有，重建不清空）。
        retry_counts: main.py 持有的推送重试计数 dict（跨重建保留）。
        poll_settings: poll 设置字典（global_min_interval_sec /
            poll_jitter_sec / push_title_change，越界值已在 normalize 钳制）。
        logger: 显式 logger；缺省用插件统一 logger。
        now: 时钟注入（可调用），测试用可控时钟。
        sleep: 睡眠注入（可调用），测试用假 sleep（确定性推进假时钟）。
        rand: ``uniform(a, b)`` 注入，测试可替换；缺省 ``random.uniform``。
        loop: 预留 event loop 参数（未使用，保持签名兼容）。
    """

    def __init__(
        self,
        subscriptions: list[Subscription],
        credential_cfg: dict[str, Any],
        repo: BiliRepository | None = None,
        db: Database | None = None,
        build_chain: Callable[[str, dict[str, Any]], Any] | None = None,
        send: Callable[..., Awaitable[dict[str, bool]]] | None = None,
        context: Any = None,
        status: dict[str, Any] | None = None,
        retry_counts: dict[str, dict[str, int]] | None = None,
        poll_settings: dict[str, Any] | None = None,
        logger: logging.Logger | None = None,
        now: Callable[[], float] | None = None,
        loop: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
        rand: Callable[[float, float], float] | None = None,
    ) -> None:
        del loop  # 预留参数：scheduler 不依赖外部 loop
        self._subscriptions: list[Subscription] = list(subscriptions)
        self._credential_cfg: dict[str, Any] = dict(credential_cfg or {})
        if repo is not None:
            self._repo: BiliRepository = repo
            self._repo_factory: Callable[[dict[str, Any]], BiliRepository] = (
                lambda _cfg: repo
            )
        else:
            self._repo = SdkRepository(self._credential_cfg)
            self._repo_factory = SdkRepository
        self.db = db
        self.build_chain = build_chain
        self.send = send
        self.context = context
        self.status: dict[str, Any] = status if status is not None else {}
        self.retry_counts: dict[str, dict[str, int]] = (
            retry_counts if retry_counts is not None else {}
        )
        self._logger = logger if logger is not None else _get_logger()
        self._now: Callable[[], float] = now if now is not None else time.monotonic
        #: 轮询器使用的 epoch 时钟：``time.monotonic`` 无纪元语义，若误传给
        #: 直播轮询器会把「下播时间/时长」算成 1970 年起的垃圾值（时长恒 0）。
        #: 令牌桶继续用 ``_now``（monotonic 抗系统时钟跳变），二者解耦。
        self._epoch_now: Callable[[], float] = now if now is not None else time.time
        self._sleep: Callable[[float], Awaitable[None]] = (
            sleep if sleep is not None else asyncio.sleep
        )
        self._rand: Callable[[float, float], float] = (
            rand if rand is not None else random.uniform
        )
        self._global_min = 60.0
        self._jitter = 0.0
        self._push_title_change = True
        self._push_live_cover = True
        self._push_dynamic_cover = True
        self._push_collection_cover = True
        self._push_dynamic_live_share = False
        #: 最近一次打印过的推送开关摘要（变更时打印，供面板保存后即时确认）。
        self._last_push_summary: str | None = None
        self._apply_poll_settings(poll_settings)

        self._bucket = self._new_bucket()
        self._tasks: list[asyncio.Task[None]] = []
        self._maintenance_task: asyncio.Task[None] | None = None
        self._consecutive_errors: dict[str, int] = {}
        self._lock = asyncio.Lock()
        self._started = False
        #: 当前 poller 映射（sub_id -> poller），供测试与状态面板访问。
        self.pollers: dict[str, Any] = {}

    # ------------------------------------------------------------------
    # 配置 / 生命周期
    # ------------------------------------------------------------------

    def _apply_poll_settings(self, poll_settings: dict[str, Any] | None) -> None:
        """应用 poll 设置（数值在 config.normalize 已钳制，此处再防御）。"""
        settings = poll_settings or {}
        raw_min = settings.get("global_min_interval_sec", 60)
        raw_jitter = settings.get("poll_jitter_sec", 0)
        try:
            self._global_min = max(1.0, float(raw_min))
        except (TypeError, ValueError):
            self._global_min = 60.0
        try:
            self._jitter = max(0.0, float(raw_jitter))
        except (TypeError, ValueError):
            self._jitter = 0.0
        self._push_title_change = coerce_bool(settings.get("push_title_change"), True)
        self._push_live_cover = coerce_bool(settings.get("push_live_cover"), True)
        self._push_dynamic_cover = coerce_bool(settings.get("push_dynamic_cover"), True)
        self._push_collection_cover = coerce_bool(
            settings.get("push_collection_cover"), True
        )
        self._push_dynamic_live_share = coerce_bool(
            settings.get("push_dynamic_live_share"), False
        )
        summary = self.push_settings_summary()
        if summary != self._last_push_summary:
            self._last_push_summary = summary
            self._logger.info("推送开关：%s", summary)

    def push_settings_summary(self) -> str:
        """返回推送开关摘要（启动日志与排查用）。"""

        def state(flag: bool) -> str:
            return "开" if flag else "关"

        return (
            f"直播封面={state(self._push_live_cover)} "
            f"动态封面={state(self._push_dynamic_cover)} "
            f"合集封面={state(self._push_collection_cover)} "
            f"直播分享动态={state(self._push_dynamic_live_share)}"
        )

    def _new_bucket(self) -> TokenBucket:
        """按当前订阅重建令牌桶（容量 3、速率=聚合轮询需求，见 :meth:`_bucket_rate`）。"""
        return TokenBucket(
            _BUCKET_CAPACITY,
            self._bucket_rate(),
            now=self._now,
            sleep=self._sleep,
        )

    def _bucket_rate(self) -> float:
        """令牌桶速率：全部启用订阅的聚合轮询需求，不低于 ``1/global_min``。

        每个订阅每轮只取一枚令牌（per-poll 限速），因此速率按
        ``sum(1 / max(poll_interval_sec, global_min))`` 恰好覆盖配置的轮询
        频率——桶不会成为瓶颈，各订阅实际间隔贴近配置值；桶容量 3 仅用于
        吸收多订阅对齐瞬间的突发。
        """
        demand = 0.0
        for sub in self._subscriptions:
            if sub.enabled:
                demand += 1.0 / max(float(sub.poll_interval_sec), self._global_min)
        return max(demand, 1.0 / self._global_min)

    def _build_pollers(self) -> dict[str, Any]:
        """按当前订阅构造 poller 映射；每类订阅注入同一把桶的取牌函数。"""
        pollers: dict[str, Any] = {}
        acquire = self._bucket.acquire
        for sub in self._subscriptions:
            poller: Any | None = None
            if sub.type == "live":
                poller = LivePoller(
                    sub,
                    self._repo,
                    self.db,
                    self.build_chain,
                    self.send,
                    self.status,
                    self._logger,
                    self.context,
                    self._push_title_change,
                    now=self._epoch_now,
                    acquire=acquire,
                    push_cover=self._push_live_cover,
                )
            elif sub.type == "dynamic":
                poller = DynamicPoller(
                    sub,
                    self._repo,
                    self.db,
                    self.build_chain,
                    self.send,
                    self.context,
                    self.status,
                    self.retry_counts,
                    self._logger,
                    acquire=acquire,
                    push_cover=self._push_dynamic_cover,
                    push_live_share=self._push_dynamic_live_share,
                )
            elif sub.type == "collection":
                poller = CollectionPoller(
                    sub,
                    self._repo,
                    self.db,
                    self.build_chain,
                    self.send,
                    self.context,
                    self.status,
                    self.retry_counts,
                    self._logger,
                    acquire=acquire,
                    push_cover=self._push_collection_cover,
                )
            if poller is not None:
                pollers[sub.id] = poller
        return pollers

    def start(self) -> None:
        """为每个订阅创建独立轮询任务（幂等）。"""
        if self._started:
            return
        self._started = True
        self.pollers = self._build_pollers()
        for sub in self._subscriptions:
            self._tasks.append(
                asyncio.create_task(self._run_sub(sub), name=f"bili-{sub.id[:8]}")
            )

    async def stop(self) -> None:
        """取消并等待全部任务（3 个轮询 + 维护任务）；调用方取消时重抛。

        轮询任务的 ``CancelledError`` 被吸收（正常 shutdown）；若 stop 本身
        在等待中被取消，``CancelledError`` 自然向外传播。
        """
        await self._cancel_tasks(
            self._tasks + ([self._maintenance_task] if self._maintenance_task else [])
        )
        self._tasks = []
        self._maintenance_task = None
        self._started = False

    async def _cancel_tasks(self, tasks: list[asyncio.Task[Any]]) -> None:
        """取消任务列表并等待完成（吸收子任务 CancelledError）。"""
        for task in tasks:
            if not task.done():
                task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

    async def rebuild(
        self,
        new_subs: list[Subscription],
        new_poll_settings: dict[str, Any] | None = None,
        clear_disabled: bool = False,
    ) -> None:
        """热重建：取消旧轮询任务、重建桶与 poller、按新配置重启。

        ``status`` / ``retry_counts`` / auto-disable 标志一律保留
        （仅重启清空）；``clear_disabled=True`` 时额外清空运行时禁用标志
        （WebUI 保存传 True、watcher 传 False）。维护任务不受影响。
        """
        async with self._lock:
            if self._started:
                await self._cancel_tasks(self._tasks)
                self._tasks = []
                self._started = False
            self._subscriptions = list(new_subs)
            self._apply_poll_settings(new_poll_settings)
            if clear_disabled:
                self.clear_disabled()
            self._repo = self._repo_factory(self._credential_cfg)
            self._bucket = self._new_bucket()
            self.start()

    def clear_disabled(self) -> None:
        """清空全部运行时自动禁用标志与连续错误计数（廉价，无任务搅动）。"""
        for sub_id, entry in self.status.items():
            if getattr(entry, "auto_disabled", False):
                entry.auto_disabled = False
        self._consecutive_errors.clear()

    def get_subscriptions(self) -> list[Subscription]:
        """返回当前订阅快照的副本（热重建后保持最新）。

        Returns:
            当前 :class:`Subscription` 列表副本；供启动日志与平台指令查询使用。
        """
        return list(self._subscriptions)

    async def check_login(self) -> dict[str, Any] | None:
        """校验 B 站凭据登录状态（若 repository 支持）；不支持时返回 None。

        Returns:
            已登录用户信息 dict（含 ``mid`` / ``uname``）；repository 未实现
            ``check_login``（如测试注入的 fake）时返回 None。
        """
        checker = getattr(self._repo, "check_login", None)
        if not callable(checker):
            return None
        return await checker()

    # ------------------------------------------------------------------
    # 维护任务（main.py 持有独立稳定引用，不随 rebuild 重建）
    # ------------------------------------------------------------------

    async def maintenance_loop(self) -> None:
        """每 6 小时调用一次 ``db.prune_old()``；异常吞掉记日志不崩溃。"""
        while True:
            await self._sleep(_MAINTENANCE_INTERVAL_SEC)
            try:
                await self.db.prune_old()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 维护任务不允许未捕获异常
                self._logger.error("维护任务 prune_old 失败: %s", exc, exc_info=True)

    def create_maintenance_task(self) -> asyncio.Task[None]:
        """创建（或复用）维护任务并返回稳定引用（main.py 调用一次）。"""
        if self._maintenance_task is None or self._maintenance_task.done():
            self._maintenance_task = asyncio.create_task(
                self.maintenance_loop(), name="bili-maintenance"
            )
        return self._maintenance_task

    # ------------------------------------------------------------------
    # 轮询循环
    # ------------------------------------------------------------------

    async def _run_sub(self, sub: Subscription) -> None:
        """单个订阅的独立轮询循环：按自身间隔轮询，互不影响。

        每轮先按 ``max(poll_interval_sec, global_min) + jitter`` 睡眠，再检查
        启用状态（enabled / 运行时自动禁用）决定是否轮询——被禁用的订阅也按
        自身间隔重查，重新启用后无需重建即可恢复。``CancelledError`` 透传；
        其余任何异常（含轮询后的账务逻辑）都记入错误计数并继续循环，绝不
        让单个异常永久杀死该订阅的轮询任务。
        """
        while True:
            await self._sleep(self._interval_for(sub))
            if not sub.enabled or self._is_auto_disabled(sub.id):
                continue
            poller = self.pollers.get(sub.id)
            if poller is None:
                continue
            try:
                await self._poll_one(sub, poller)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 账务异常不允许杀死轮询任务
                self._logger.error(
                    "轮询任务异常（sub=%s），已记录并继续: %s",
                    sub.name,
                    exc,
                    exc_info=True,
                )
                entry = self._ensure_status(sub.id)
                await self._record_error(sub, entry)

    def _interval_for(self, sub: Subscription) -> float:
        """订阅单轮间隔：``max(poll_interval_sec, global_min) + uniform(0, jitter)``。"""
        interval = max(float(sub.poll_interval_sec), self._global_min)
        return interval + self._rand(0.0, self._jitter)

    async def _poll_one(self, sub: Subscription, poller: Any) -> None:
        """执行一轮轮询并维护 status：last_poll/错误计数/退避/自动禁用。

        错误信号：轮询前置 ``last_error`` 为 None，轮询/推送失败会立即重写
        （pollers 与 push.send 均写该字段），轮后非 None 即本轮出错；另以
        ``poller.error_count`` 增量兜底（LivePoller 持有）。
        """
        entry = self._ensure_status(sub.id)
        entry.last_poll = _now_iso()
        entry.last_error = None
        errors_before = int(getattr(poller, "error_count", 0))
        try:
            await poller.poll()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._logger.error(
                "调度器轮询异常（sub=%s）: %s", sub.name, exc, exc_info=True
            )
            errored = True
        else:
            errors_after = int(getattr(poller, "error_count", 0))
            errored = entry.last_error is not None or errors_after != errors_before
        if errored:
            await self._record_error(sub, entry)
        else:
            self._consecutive_errors.pop(sub.id, None)
        if sub.type == "live":
            state = await self.db.get_live_state(sub.id)
            entry.live_status = (
                int(state.last_status)
                if state is not None and state.last_status is not None
                else None
            )

    async def _record_error(self, sub: Subscription, entry: Any) -> None:
        """累计错误计数；连续失败达 N 次自动禁用，否则指数退避睡眠。"""
        count = self._consecutive_errors.get(sub.id, 0) + 1
        self._consecutive_errors[sub.id] = count
        entry.error_count = int(getattr(entry, "error_count", 0)) + 1
        if count >= _MAX_CONSECUTIVE_ERRORS and not getattr(
            entry, "auto_disabled", False
        ):
            await self._auto_disable(sub, entry)
            return
        backoff = min(_BACKOFF_BASE**count, _BACKOFF_MAX_SEC)
        self._logger.warning(
            "订阅 %s 轮询连续失败 %d 次，退避 %.0fs", sub.name, count, backoff
        )
        await self._sleep(backoff)

    async def _auto_disable(self, sub: Subscription, entry: Any) -> None:
        """自动禁用订阅（仅运行时标志，重启恢复）并向其全部会话推送告警。"""
        entry.auto_disabled = True
        self._logger.warning(
            "订阅 %s 连续失败 %d 次，已自动禁用（仅运行时，重启后恢复）",
            sub.name,
            _MAX_CONSECUTIVE_ERRORS,
        )
        payload: dict[str, Any] = {
            "name": sub.name,
            "type_text": "自动禁用告警",
            "content": (
                f"订阅连续轮询失败 {_MAX_CONSECUTIVE_ERRORS} 次，"
                "已自动禁用该订阅（重启插件后恢复）。"
            ),
        }
        if sub.uid is not None:
            payload["url"] = f"https://space.bilibili.com/{sub.uid}"
        chain = self.build_chain("dynamic", payload)
        await self.send(sub, chain, self.context, self.status)

    # ------------------------------------------------------------------
    # 内部工具
    # ------------------------------------------------------------------

    def _ensure_status(self, sub_id: str) -> Any:
        """返回 sub_id 的 status 条目，缺失时创建（含调度器维护字段）。"""
        entry = self.status.get(sub_id)
        if entry is None:
            entry = SimpleNamespace(
                last_push_at=None,
                last_error=None,
                error_count=0,
                last_poll=None,
                live_status=None,
                auto_disabled=False,
            )
            self.status[sub_id] = entry
        return entry

    def _is_auto_disabled(self, sub_id: str) -> bool:
        """判断订阅是否被运行时自动禁用（每轮检查）。"""
        entry = self.status.get(sub_id)
        return bool(entry is not None and getattr(entry, "auto_disabled", False))
