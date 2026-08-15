"""配置热重载器（从 main.py 拆出的独立模块，计划 todo 13）。

无 AstrBot 依赖：单一共享 200ms 防抖窗口 + 串行化重建（同一时刻至多一个
重建在跑）、三态返回（``parse-failed`` / ``no-op`` / ``rebuilt``）、磁盘
重读比对、身份变更状态清理、save-then-swap 持久化，以及独立的 5s 配置
watcher 任务。

重建的唯一入口是 :meth:`ConfigReloader.request_rebuild`；调度器与轮询任务
的启停由 :mod:`lifecycle` 负责，本模块只负责"何时重建、如何清理状态"。
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any, Literal

try:
    from . import config_file, util
    from .config import Subscription, normalize
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    import config_file  # type: ignore[import-not-found]
    import util  # type: ignore[import-not-found]
    from config import Subscription, normalize  # type: ignore[import-not-found]

#: 重建防抖窗口（秒）：窗口内新请求顺延/合并，到期后合并为一次重建。
REBUILD_DEBOUNCE_SEC: float = 0.2
#: 配置 watcher 轮询间隔（秒）。
WATCH_INTERVAL_SEC: float = 5.0

#: ``request_rebuild`` 的三态返回：解析失败 / 配置一致未重建 / 已实际重建。
RebuildResult = Literal["parse-failed", "no-op", "rebuilt"]


def _identity_changed(old: dict[str, Any], new: Subscription) -> bool:
    """判断订阅身份（type/uid/list_id/series_type）是否变化。

    身份变化强制重 seed：旧 ``room_id`` 缓存、动态/合集去重历史与 seed 标志
    全部失效，必须清库重扫，避免旧 room_id 被误轮询、死 sub 残留状态。
    """
    return (
        old.get("type") != new.type
        or old.get("uid") != new.uid
        or old.get("list_id") != new.list_id
        or old.get("series_type") != new.series_type
    )


class ConfigReloader:
    """配置热重载器：防抖 + 串行化 + 磁盘比对重建（计划 todo 13）。

    重建的唯一入口是 :meth:`request_rebuild`：

    - **防抖**：单一共享 200ms 窗口，窗口内连续请求合并为一次重建；同一时刻
      至多一个重建在跑，重建期间到达的新请求合并到下一个防抖轮次。
    - **三态返回**：``parse-failed``（磁盘读取/JSON 解析/normalize 失败，未动
      当前任务）、``no-op``（读取成功但与 :attr:`_active_config` 快照一致，
      未重建）、``rebuilt``（已实际重建）。
    - **比对基准**：持有完整的规范化配置快照（subscriptions + credential +
      poll），不是实时 ``self.config``——WebUI 写盘后重建不会误跳过；
      settings（轮询/凭据）变更同样触发重建。
    - **锁**：``self.lock`` 与 WebUI 的配置写锁是**同一把锁**（todo 11 约定），
      锁范围仅包住重建全程（重读→比对→清理→重建→持久化）；WebUI 的
      normalize+save_config_async 也在这把锁内，释放后才调 request_rebuild
      （非重入，绝不可跨 request_rebuild 持锁）。

    Args:
        config_path: 插件配置文件路径；None 时按 AstrBot 约定解析。
        scheduler: 持有 3 个轮询任务与维护任务的 Scheduler 实例
            （重建只经 ``rebuild()``，watcher/维护任务绝不被触碰）。
        db: 数据层 Database（身份变更时 ``delete_sub_state`` 清库）。
        status: 与 Scheduler 共享的 runtime status dict（按 sub_id）。
        retry_counts: 与 Scheduler 共享的推送重试计数 dict（按 sub_id）。
        config_writer: 持久配置写入器（生产为 AstrBotConfig 实例，须有
            ``save_config_async(replace_config=None)``）；None 时跳过持久化。
        logger: 显式 logger；缺省用插件统一 logger。
        debounce_sec: 防抖窗口（秒），默认 0.2。
        watch_interval_sec: watcher 轮询间隔（秒），默认 5。
        sleep: 睡眠注入（可调用），测试用假 sleep（全确定性）；缺省
            ``asyncio.sleep``。
    """

    def __init__(
        self,
        config_path: str | Path | None = None,
        scheduler: Any = None,
        db: Any = None,
        status: dict[str, Any] | None = None,
        retry_counts: dict[str, dict[str, int]] | None = None,
        config_writer: Any | None = None,
        logger: Any | None = None,
        debounce_sec: float = REBUILD_DEBOUNCE_SEC,
        watch_interval_sec: float = WATCH_INTERVAL_SEC,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._config_path: Path = (
            Path(config_path)
            if config_path is not None
            else config_file._default_config_path()
        )
        self._scheduler: Any = scheduler
        self._db: Any = db
        self._status: dict[str, Any] = status if status is not None else {}
        self._retry_counts: dict[str, dict[str, int]] = (
            retry_counts if retry_counts is not None else {}
        )
        self._config_writer: Any = config_writer
        self._logger: Any = logger if logger is not None else util.get_logger(__name__)
        self._debounce_sec: float = debounce_sec
        self._watch_interval_sec: float = watch_interval_sec
        self._sleep: Callable[[float], Awaitable[None]] = (
            sleep if sleep is not None else asyncio.sleep
        )

        #: 重建锁：与 WebUI 的配置写锁为同一把（todo 11 约定）。
        self.lock: asyncio.Lock = asyncio.Lock()
        self._closing: bool = False
        #: 当前轮询任务所基于的完整规范化配置快照（重建比对基准，见类文档）。
        self._active_config: dict[str, Any] | None = None

        # 防抖状态：_pending 表示存在未服务的重建请求；_waiters 为等待本次
        # 合并重建结果的 future 列表；_loop_task 为共享防抖循环任务。
        self._pending: bool = False
        self._pending_clear_disabled: bool = False
        self._waiters: list[asyncio.Future[str]] = []
        self._loop_task: asyncio.Task[None] | None = None

        # watcher 状态：_watcher_snapshot 为上次成功处理后的 (size, mtime_ns)。
        self._watcher_task: asyncio.Task[None] | None = None
        self._watcher_snapshot: tuple[int, int] | None = None

        #: 最近一次配置读取失败信息（None = 最近读取成功/尚未读取），供 WebUI 展示。
        self._config_error: str | None = None
        #: 最近一次已告警的失败信息（去重，避免 watcher 每 5s 刷屏）。
        self._last_logged_error: str | None = None

    # ------------------------------------------------------------------
    # 重建入口（唯一）
    # ------------------------------------------------------------------

    async def request_rebuild(self, clear_disabled: bool = False) -> RebuildResult:
        """请求热重建（防抖合并 + 串行化，唯一重建入口）。

        本方法注册一个请求后等待合并轮次完成，返回该轮的三态结果：
        ``parse-failed`` / ``no-op`` / ``rebuilt``。WebUI 保存传
        ``clear_disabled=True``（WebUI 保存传 True、watcher 传 False）；
        防抖合并多个请求时 clear_disabled 取 OR（任一 True 即清空）。

        Args:
            clear_disabled: 是否清空运行时自动禁用标志（no-op 时也生效）。

        Returns:
            三态结果；``_closing`` 为 True 时直接返回 ``"no-op"``（不触碰任务）。
        """
        async with self.lock:
            if self._closing:
                return "no-op"
            self._pending = True
            self._pending_clear_disabled = (
                self._pending_clear_disabled or clear_disabled
            )
            if self._loop_task is None or self._loop_task.done():
                self._loop_task = asyncio.create_task(self._debounce_loop())
            future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
            self._waiters.append(future)
        return await future

    async def _debounce_loop(self) -> None:
        """防抖循环：窗口内无新请求才执行一次重建；期间新请求进入下一轮。

        每个合并轮次服务注册时挂入 ``_waiters`` 的请求，把该轮的三态结果
        写入每个请求的 future。请求在重建阶段（快照互换之后）到达时归属
        下一轮，保证每个请求都拿到自己所在轮次的结果。
        """
        try:
            while True:
                # 防抖窗口（锁外等待）：持续有新请求就顺延一个窗口。
                while self._pending:
                    self._pending = False
                    await self._sleep(self._debounce_sec)
                async with self.lock:
                    my_waiters, self._waiters = self._waiters, []
                result = await self._run_rebuild()
                for future in my_waiters:
                    if not future.done():
                        future.set_result(result)
                async with self.lock:
                    if self._pending:
                        continue
                    # 锁内原子退出：避免与 registration 竞态导致请求漏服务。
                    if self._loop_task is asyncio.current_task():
                        self._loop_task = None
                    return
        except asyncio.CancelledError:
            raise
        finally:
            async with self.lock:
                if self._loop_task is asyncio.current_task():
                    self._loop_task = None
                waiters, self._waiters = self._waiters, []
                if waiters and not self._closing:
                    # 轮次收尾与任务结束之间到达的请求：重开一轮服务它们，
                    # 否则请求会带着 "no-op" 空手而归、漏掉一次重建。
                    self._waiters = waiters
                    self._loop_task = asyncio.create_task(self._debounce_loop())
                else:
                    for future in waiters:
                        if not future.done():
                            future.set_result("no-op")

    async def _run_rebuild(self) -> RebuildResult:
        """防抖到期后执行一次实际重建（锁内串行化）。"""
        async with self.lock:
            if self._closing:
                # 关闭期间到达的合并轮次：不触碰任何任务。
                return "no-op"
            clear = self._pending_clear_disabled
            self._pending_clear_disabled = False
            try:
                return await self._rebuild_locked(clear)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 重建失败不应当崩溃防抖循环
                self._logger.error(
                    "配置重建异常（保留当前任务）: %s", exc, exc_info=True
                )
                return "parse-failed"

    async def _rebuild_locked(self, clear: bool) -> RebuildResult:
        """锁内重建：重读磁盘 → 比对快照 → 清理身份变更 → 重建 → 持久化。

        调用方已持有 :attr:`lock` 且已检查 ``_closing``。
        """
        read = self._read_and_normalize()
        if read is None:
            return "parse-failed"
        raw, subs = read
        snapshot = self._build_snapshot(raw, subs)
        if self._active_config is not None and snapshot == self._active_config:
            if clear:
                # 重建被跳过时仍应用 clear_disabled（廉价清标志、无任务搅动）。
                self._scheduler.clear_disabled()
            return "no-op"
        if self._active_config is not None:
            await self._cleanup_removed_or_changed(subs)
        # scheduler.rebuild 以 self._credential_cfg 重建 repository——先喂最新
        # 凭据（settings 变更也触发重建，Cookie 更新才能生效）。
        credential = raw.get("credential")
        self._scheduler._credential_cfg = (
            dict(credential) if isinstance(credential, dict) else {}
        )
        await self._scheduler.rebuild(subs, raw.get("poll"), clear_disabled=clear)
        # 先持久化（新分配的订阅 id 落盘）再换 _active_config，下一次比对才稳定。
        await self._persist_config(raw, subs)
        self._active_config = snapshot
        return "rebuilt"

    # ------------------------------------------------------------------
    # 配置读取 / 快照
    # ------------------------------------------------------------------

    def _read_and_normalize(self) -> tuple[dict[str, Any], list[Subscription]] | None:
        """重读磁盘配置并 normalize；任何失败返回 None（保留当前任务）。

        ``AstrBotConfig`` 无 reload 方法：直接用 ``json.load`` 读文件为 dict，
        再跑 :func:`normalize`；不构造 ``AstrBotConfig``，规避
        ``check_config_integrity`` 的裁剪/副作用。AstrBot 以 ``utf-8-sig`` 落盘
        （文件带 UTF-8 BOM），此处同样以 ``utf-8-sig`` 读取以兼容 BOM。
        """
        try:
            with open(self._config_path, "r", encoding="utf-8-sig") as f:
                raw = json.load(f)
        except (OSError, ValueError) as exc:
            self._record_config_failure(f"读取失败: {exc}")
            return None
        if not isinstance(raw, dict):
            self._record_config_failure("顶层不是对象")
            return None
        if "subscriptions" not in raw:
            # 键缺失通常来自手改笔误（键拼错/误删）；若按空列表重建并持久化，
            # 会把用户全部订阅与去重状态清空。拒绝重建、保留当前任务。
            self._record_config_failure(
                "缺少 subscriptions 键（疑似手改笔误），拒绝重建以防清空订阅"
            )
            return None
        try:
            subs = normalize(raw)
        except Exception as exc:  # noqa: BLE001 - normalize 失败不应当中断重建
            self._record_config_failure(f"normalize 失败: {exc}")
            return None
        self._config_error = None
        self._last_logged_error = None
        return raw, subs

    def _record_config_failure(self, message: str) -> None:
        """记录配置读取失败：去重告警（同类失败仅告警一次）+ 写入 WebUI 状态。

        连续失败期间 watcher 每 5s 重试，若每次都告警会刷屏；这里仅在失败信息
        变化时告警，同时把最新失败写入 ``_config_error`` 供 ``config_status``
        展示。
        """
        self._config_error = message
        if message == self._last_logged_error:
            return
        self._last_logged_error = message
        self._logger.warning(
            "重读配置 %s 失败，保留当前任务: %s", self._config_path, message
        )

    def config_status(self) -> dict[str, Any]:
        """返回配置文件健康状态（供 WebUI ``/api/config-status`` 展示）。

        Returns:
            ``{"path", "ok", "last_error"}``；``ok`` 为 False 时 ``last_error``
            为最近一次读取失败原因。
        """
        return {
            "path": str(self._config_path),
            "ok": self._config_error is None,
            "last_error": self._config_error,
        }

    @staticmethod
    def _build_snapshot(
        raw: dict[str, Any], subs: list[Subscription]
    ) -> dict[str, Any]:
        """构建重建比对基准快照：完整规范化有效配置（订阅 + 凭据 + 轮询设置）。

        ``poll`` 已被 normalize 就地钳制；凭据变更（改 Cookie）与轮询设置变更
        都会使快照不同 → 触发重建。
        """
        return {
            "subscriptions": [sub.to_dict() for sub in subs],
            "credential": raw.get("credential"),
            "poll": raw.get("poll"),
        }

    def seed_active_config(self, raw: dict[str, Any], subs: list[Subscription]) -> None:
        """以启动配置初始化重建比对快照（T14 的 initialize 调用，幂等）。

        使 WebUI 保存内容与启动配置一致时，首轮重建正确判定为 ``no-op``
        （无需取消重启轮询任务）；watcher 自身以文件 (size, mtime) 为准，
        不受本快照影响。
        """
        self._active_config = self._build_snapshot(raw, subs)

    async def _persist_config(
        self, raw: dict[str, Any], subs: list[Subscription]
    ) -> None:
        """用规范化结果更新持久配置内存值并落盘（无写入器时跳过）。"""
        if self._config_writer is None:
            return
        persisted = dict(raw)
        persisted["subscriptions"] = [sub.to_dict() for sub in subs]
        await self._config_writer.save_config_async(persisted)

    # ------------------------------------------------------------------
    # 身份变更清理
    # ------------------------------------------------------------------

    async def _cleanup_removed_or_changed(self, new_subs: list[Subscription]) -> None:
        """清理被删除或身份变更的订阅状态（身份/类型变化强制重 seed）。

        对新配置中已不存在、或 type/uid/list_id/series_type 变化的 sub_id：
        删除其全部持久行（``db.delete_sub_state``）+ runtime status 条目 +
        retry 计数条目，避免旧 room_id 被误轮询、死 sub 残留 /api/status。
        """
        old_subs = self._active_config["subscriptions"]  # type: ignore[index]
        old_by_id = {old["id"]: old for old in old_subs}
        new_by_id = {sub.id: sub for sub in new_subs}
        for sub_id, old in old_by_id.items():
            new = new_by_id.get(sub_id)
            if new is None or _identity_changed(old, new):
                await self._purge_sub(sub_id)

    async def _purge_sub(self, sub_id: str) -> None:
        """删除订阅的全部持久状态与运行时状态（身份/类型变化强制重 seed）。"""
        await self._db.delete_sub_state(sub_id)
        self._status.pop(sub_id, None)
        self._retry_counts.pop(sub_id, None)

    # ------------------------------------------------------------------
    # 配置 watcher（T14 在 initialize 中 start，terminate 中 shutdown）
    # ------------------------------------------------------------------

    async def watcher_loop(self) -> None:
        """独立配置 watcher：每 5s 比对 (size, mtime)，变化则触发重建。

        外部手动编辑 JSON 也能热重载。快照刷新规则（r21/r22 W1 修复）：

        - 任何一次成功的读取+JSON 解析+normalize（``request_rebuild`` 返回
          ``no-op`` 或 ``rebuilt``）后都刷新 (size, mtime) 快照；
        - 仅当读取/解析失败（``parse-failed``，撕裂读/非原子写盘）时才保留旧
          快照，让下一个 5s tick 重试——否则每次 WebUI 保存后 watcher 下一
          tick 都会永久重试（no-op 不刷新快照的旧措辞已废弃）。
        """
        self._refresh_watcher_snapshot()  # 启动时初始化快照，避免误触发
        while True:
            await self._sleep(self._watch_interval_sec)
            if self._closing:
                return
            current = self._stat_config()
            if current is None:
                # 文件暂不可读（未创建/正被替换）：保留旧快照，下轮重试。
                continue
            if current == self._watcher_snapshot:
                continue
            result = await self.request_rebuild()
            if result != "parse-failed":
                self._refresh_watcher_snapshot()

    def start_watcher(self) -> asyncio.Task[None]:
        """创建（或复用）watcher 任务并返回稳定引用；T14 的 initialize 调用。"""
        if self._watcher_task is None or self._watcher_task.done():
            self._watcher_task = asyncio.create_task(
                self.watcher_loop(), name="bili-config-watcher"
            )
        return self._watcher_task

    async def stop_watcher(self) -> None:
        """取消并等待 watcher 任务（幂等）；T14 的 terminate 调用。"""
        task, self._watcher_task = self._watcher_task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def shutdown(self) -> None:
        """置 ``_closing`` 并停掉 watcher 与防抖循环；T14 的 terminate 调用。

        防抖循环被取消后其 finally 会把未服务的请求以 ``no-op`` 结算，不会
        在关闭后触发新的重建。
        """
        self._closing = True
        await self.stop_watcher()
        async with self.lock:
            task = self._loop_task
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    def reset(self) -> None:
        """复位关闭标志（terminate 后同一实例重新 initialize 时恢复热重载）。

        ``shutdown()`` 置 ``_closing=True`` 后 ``request_rebuild`` 会永远返回
        ``no-op``、watcher 一轮即退出——复用时必须先调用本方法复位。
        """
        self._closing = False

    def _stat_config(self) -> tuple[int, int] | None:
        """返回 (size, mtime_ns) 快照；stat 失败（文件不存在等）返回 None。

        mtime 用纳秒精度，避免同秒内两次写入（同尺寸）被漏检。
        """
        try:
            st = self._config_path.stat()
        except OSError:
            return None
        return st.st_size, int(st.st_mtime_ns)

    def _refresh_watcher_snapshot(self) -> None:
        """把 watcher 快照更新为当前文件状态（stat 失败则保留旧值）。"""
        current = self._stat_config()
        if current is not None:
            self._watcher_snapshot = current
