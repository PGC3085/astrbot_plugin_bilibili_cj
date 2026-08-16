"""插件生命周期接线（从 main.py 拆出的独立模块，计划 todo 14）。

**无 Star/AstrBot 依赖**：依赖全部注入（``config`` / ``context`` / ``db`` /
``scheduler`` / ``reloader`` / ``webui``），:class:`BilibiliMonitor` 仅委托
本模块完成 ``initialize()`` / ``terminate()``；离线测试可注入 fake 组件全量
驱动。

启动顺序：``db.init()`` → 凭据检查（无 sessdata 告警匿名模式）→ WebUI
（仅 ``webui.enabled``，端口绑定失败降级禁用不阻断）→ ``scheduler.start()``
+ ``create_maintenance_task()``（独立稳定引用）→ ``reloader.start_watcher()``。

关停顺序（全部幂等，``asyncio.CancelledError`` 不捕获自然重抛）：
置 ``_closing`` → 先停登录监控 → ``reloader.shutdown()``（先 cancel watcher
阻止新重建）→ 重建锁内 ``scheduler.stop()`` → ``webui.stop()``（释放端口 +
移除日志 Handler）→ ``db.close()``。
"""

from __future__ import annotations

import asyncio
import math
from pathlib import Path
from typing import Any, Awaitable, Callable

try:
    from . import config_file, config_reloader, push, util
    from .config import Subscription, coerce_bool, normalize
    from .db import Database
    from .scheduler import Scheduler
    from .webui.server import WebUIServer
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    import config_file  # type: ignore[import-not-found]
    import config_reloader  # type: ignore[import-not-found]
    import push  # type: ignore[import-not-found]
    import util  # type: ignore[import-not-found]
    from config import Subscription, coerce_bool, normalize  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from scheduler import Scheduler  # type: ignore[import-not-found]
    from webui.server import WebUIServer  # type: ignore[import-not-found]

#: 登录关键凭据字段（启动时检查，缺失告警）。
_REQUIRED_CREDENTIAL_FIELDS: tuple[str, ...] = ("sessdata", "bili_jct", "dedeuserid")
#: 可选凭据字段（缺失不告警）：buvid3/buvid4 为设备指纹，ac_time_value 仅用于刷新。
_OPTIONAL_CREDENTIAL_FIELDS: tuple[str, ...] = ("buvid3", "buvid4", "ac_time_value")

#: 登录状态监控默认校验间隔（秒）。
_LOGIN_MONITOR_DEFAULT_INTERVAL: int = 3600
#: 登录状态监控默认失败通知阈值（连续失败次数）。
_LOGIN_MONITOR_DEFAULT_THRESHOLD: int = 3
#: 登录校验间隔下限（秒，低于此值钳制）。
_LOGIN_MONITOR_MIN_INTERVAL: int = 60
#: 登录失败通知阈值下限（低于此值钳制）。
_LOGIN_MONITOR_MIN_THRESHOLD: int = 1


class PluginLifecycle:
    """插件生命周期接线（计划 todo 14）：**无 Star/AstrBot 依赖**。

    Args:
        config: 持久化配置（AstrBotConfig dict 子类或 dict-like）。
        context: AstrBot Context（或 duck：``send_message(session, chain)``）。
        credential_cfg: B 站凭据配置 dict（scheduler 构建 repository 用）。
        db: 数据层（``init()`` / ``close()``）。
        scheduler: 调度器（``start()`` / ``stop()`` / ``create_maintenance_task()``）。
        reloader: :class:`ConfigReloader`（``lock`` / ``shutdown()`` /
            ``start_watcher()`` / ``request_rebuild()``）。
        logger: 显式 logger；缺省用插件统一 logger。
        webui: :class:`WebUIServer` 实例；None 时按 ``webui.enabled`` 在
            initialize 中构建。
        sleep: 睡眠注入（可调用），测试用假 sleep（确定性推进假时钟）。
    """

    def __init__(
        self,
        *,
        config: Any,
        context: Any,
        credential_cfg: dict[str, Any],
        db: Any,
        scheduler: Any,
        reloader: Any,
        logger: Any | None = None,
        webui: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.config = config
        self.context = context
        self._credential_cfg: dict[str, Any] = dict(credential_cfg)
        self.db = db
        self.scheduler = scheduler
        self.reloader = reloader
        self._logger: Any = logger if logger is not None else util.get_logger(__name__)
        self.webui: Any = webui
        self._sleep: Callable[[float], Awaitable[None]] = (
            sleep if sleep is not None else asyncio.sleep
        )
        self._closing: bool = False
        #: 与 scheduler.status 共享的 runtime status dict（initialize 后可用）。
        self.status: dict[str, Any] = {}
        #: 维护任务稳定引用（scheduler.stop 内部持同一任务，此处仅观测用）。
        self._maintenance_task: asyncio.Task[None] | None = None
        #: config watcher 任务稳定引用。
        self._watcher_task: asyncio.Task[None] | None = None
        #: 登录状态监控任务稳定引用（周期性校验，terminate 时取消）。
        self._login_monitor_task: asyncio.Task[None] | None = None
        #: 最近一次登录校验通过时间（UTC ISO；None = 尚未通过）。
        self._login_last_ok_at: str | None = None
        #: 连续登录校验失败次数。
        self._login_consecutive_failures: int = 0
        #: 最近一次登录校验失败原因（None = 无失败）。
        self._login_last_error: str | None = None

    # ------------------------------------------------------------------
    # 生产工厂（真实组件装配）
    # ------------------------------------------------------------------

    @classmethod
    def create(
        cls,
        *,
        config: Any,
        context: Any,
        logger: Any | None = None,
        config_path: str | Path | None = None,
    ) -> "PluginLifecycle":
        """按真实组件装配完整生命周期（BilibiliMonitor.initialize 调用）。

        顺序：解析启动配置 → :class:`Database` → :class:`Scheduler`（repo
        走 ``SdkRepository``，无 sessdata 时匿名运行）→
        :class:`ConfigReloader`（共享 status/retry_counts dict、config 为
        持久写入器、seed 启动快照）。WebUI 由 initialize 按 ``webui.enabled``
        构建（构造需 reloader.lock / request_rebuild）。

        Args:
            config: 持久化 AstrBotConfig 实例（同时作 ConfigReloader 写入器）。
            context: AstrBot Context。
            logger: 显式 logger；缺省用插件统一 logger。
            config_path: 配置文件路径；None 按 AstrBot 约定解析。

        Returns:
            装配完成的 :class:`PluginLifecycle`（尚未 initialize）。
        """
        logger = logger if logger is not None else util.get_logger(__name__)
        # 安装兜底：配置文件缺失时按 schema 默认值初始化（AstrBot 一般已创建）。
        resolved_path = (
            Path(config_path)
            if config_path is not None
            else config_file._default_config_path()
        )
        config_file.ensure_config_file(resolved_path, logger=logger)
        raw = cls._config_raw(config)
        poll = raw.get("poll")
        if isinstance(poll, dict):
            raw["poll"] = dict(poll)  # normalize 就地钳制：勿动持久配置的 poll 组
        subscriptions = normalize(raw)
        # 批量配置：仅当**现有配置没有有效订阅**时（首次部署语义）读入插件
        # 目录的 config.json 并深度合并；已有订阅说明用户已开始管理配置，
        # 重启时不再合并——否则插件目录里的旧 config.json 会在每次启动时
        # 静默覆盖用户在面板/WebUI 中的修改。
        bundled = config_file._bundled_config_path()
        if bundled is not None:
            if subscriptions:
                logger.info(
                    "已有 %d 条订阅，跳过插件目录批量配置合并（避免覆盖用户配置）",
                    len(subscriptions),
                )
            else:
                data = config_file.read_config_file(bundled, logger)
                if data is not None:
                    config_file._deep_merge(config, data)
                    saver = getattr(config, "save_config", None)
                    if callable(saver):
                        try:
                            saver()
                        except Exception as exc:  # noqa: BLE001 - 落盘失败不阻断启动
                            logger.warning("批量配置落盘失败: %s", exc)
                    logger.info("已读入插件目录批量配置文件 %s 并合并到配置", bundled)
                    # 合并后重解析：批量配置里的凭据/订阅/轮询设置立即生效。
                    raw = cls._config_raw(config)
                    poll = raw.get("poll")
                    if isinstance(poll, dict):
                        raw["poll"] = dict(poll)
                    subscriptions = normalize(raw)
        credential_raw = raw.get("credential")
        credential_cfg: dict[str, Any] = (
            dict(credential_raw) if isinstance(credential_raw, dict) else {}
        )
        db = Database()
        status: dict[str, Any] = {}
        retry_counts: dict[str, dict[str, int]] = {}
        scheduler = Scheduler(
            subscriptions=subscriptions,
            credential_cfg=credential_cfg,
            repo=None,
            db=db,
            build_chain=push.build_chain,
            send=push.send,
            context=context,
            status=status,
            retry_counts=retry_counts,
            poll_settings=raw.get("poll"),
            logger=logger,
        )
        reloader = config_reloader.ConfigReloader(
            config_path=resolved_path,
            scheduler=scheduler,
            db=db,
            status=status,
            retry_counts=retry_counts,
            config_writer=config,
            logger=logger,
        )
        # 以启动配置初始化比对快照：WebUI 保存相同内容时首轮重建判 no-op。
        reloader.seed_active_config(raw, subscriptions)
        return cls(
            config=config,
            context=context,
            credential_cfg=credential_cfg,
            db=db,
            scheduler=scheduler,
            reloader=reloader,
            logger=logger,
        )

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def initialize(self) -> None:
        """按序启动：db → WebUI（可选，失败降级）→ scheduler → watcher。"""
        self._closing = False
        await self.db.init()
        self._log_credential_status()
        self._start_login_monitor()
        if self.webui is None and self._webui_enabled():
            self.webui = self._build_webui()
        if self.webui is not None:
            host, port = self._webui_addr()
            try:
                await self.webui.start(host, port)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - WebUI 失败属降级路径
                self._logger.error(
                    "WebUI 启动失败（host=%s port=%s，已禁用，插件继续运行）: %s",
                    host,
                    port,
                    exc,
                )
                self.webui.enabled = False
            if self.webui.enabled:
                self.webui.install_log_handler()
        self.scheduler.start()
        self._maintenance_task = self.scheduler.create_maintenance_task()
        reset = getattr(self.reloader, "reset", None)
        if callable(reset):
            reset()  # 同一实例 terminate 后复用时恢复热重载能力（fake 无此方法）
        self._watcher_task = self.reloader.start_watcher()
        # /api/status 数据源：与 scheduler 共享同一个 dict（重建不清空）。
        self.status = self.scheduler.status
        # 启动完成后在 AstrBot 控制台打印订阅清单。
        self._log_subscriptions()

    def _log_credential_status(self) -> None:
        """启动时检查凭据字段完整性（buvid3/buvid4 可选，缺失不告警）。"""
        if not (self._credential_cfg.get("sessdata") or ""):
            self._logger.warning(
                "未配置 B 站 Cookie（sessdata），以匿名模式运行（仅公开接口，"
                "风控风险较高）；建议在配置中填写 Cookie 以降低风控"
            )
            return
        missing = [
            field
            for field in _REQUIRED_CREDENTIAL_FIELDS
            if not self._credential_cfg.get(field)
        ]
        if missing:
            self._logger.warning(
                "B 站凭据不完整，缺少 %s；部分接口可能受限"
                "（buvid3/buvid4 为可选，不影响）",
                "、".join(missing),
            )
        else:
            self._logger.info("B 站凭据已配置（sessdata/bili_jct/dedeuserid）。")

    # ------------------------------------------------------------------
    # 登录状态监控（周期校验 + 连续失败告警）
    # ------------------------------------------------------------------

    def _login_monitor_cfg(self) -> dict[str, Any]:
        """读取 ``login_monitor`` 配置组（缺失返回空 dict）。"""
        raw = self._config_raw(self.config)
        cfg = raw.get("login_monitor")
        return dict(cfg) if isinstance(cfg, dict) else {}

    def _login_monitor_enabled(self) -> bool:
        """是否启用登录状态监控（schema 缺省 true；字符串 "false" 不再误判为开）。"""
        return coerce_bool(self._login_monitor_cfg().get("enabled"), True)

    def _login_monitor_interval(self) -> float:
        """登录校验间隔（秒），钳制到 :data:`_LOGIN_MONITOR_MIN_INTERVAL` 以上。

        NaN/inf 等非有限数值（手改配置可写入）一律回退默认值——``sleep(inf)``
        会让监控任务永久挂起、再也不会校验登录状态。
        """
        raw = self._login_monitor_cfg().get("interval_sec")
        try:
            interval = (
                float(raw) if raw is not None else _LOGIN_MONITOR_DEFAULT_INTERVAL
            )
        except (TypeError, ValueError):
            interval = _LOGIN_MONITOR_DEFAULT_INTERVAL
        if not math.isfinite(interval):
            interval = _LOGIN_MONITOR_DEFAULT_INTERVAL
        return max(float(_LOGIN_MONITOR_MIN_INTERVAL), interval)

    def _login_monitor_threshold(self) -> int:
        """登录失败通知阈值（次），钳制到 :data:`_LOGIN_MONITOR_MIN_THRESHOLD` 以上。"""
        raw = self._login_monitor_cfg().get("fail_threshold")
        try:
            threshold = (
                int(raw) if raw is not None else _LOGIN_MONITOR_DEFAULT_THRESHOLD
            )
        except (TypeError, ValueError):
            threshold = _LOGIN_MONITOR_DEFAULT_THRESHOLD
        return max(_LOGIN_MONITOR_MIN_THRESHOLD, threshold)

    def _credential_from_config(self) -> dict[str, Any]:
        """读取当前配置中的 B 站凭据（运行时通过 WebUI/面板补填 Cookie 后
        也能读到最新值；``_credential_cfg`` 仅是 create() 时刻的快照）。"""
        raw = self._config_raw(self.config)
        credential = raw.get("credential")
        return dict(credential) if isinstance(credential, dict) else {}

    def _start_login_monitor(self) -> None:
        """启动登录状态监控任务（幂等；``enabled=false`` 时不启动）。

        凭据存在性在**循环内**逐轮检查（读当前配置）：匿名模式下任务空转
        等待，运行时补填 Cookie 并保存后监控自动开始校验，无需重载插件。
        """
        if not self._login_monitor_enabled():
            return
        if self._login_monitor_task is None or self._login_monitor_task.done():
            self._login_monitor_task = asyncio.create_task(
                self._login_monitor_loop(), name="bili-login-monitor"
            )

    async def _login_monitor_loop(self) -> None:
        """周期性校验 B 站登录状态；连续失败达阈值时通知指定会话。

        首次进入立即校验一次，之后每 ``interval_sec`` 校验一次；失败累计连续
        次数、达阈值时发送告警；成功后清零并记录通过时间。每轮重读配置：
        关闭监控（``enabled=false``）后自然退出；未配置 sessdata 时跳过本轮
        校验（空转等待，补填 Cookie 后自动恢复）。
        """
        while True:
            if not self._login_monitor_enabled():
                return
            if not (self._credential_from_config().get("sessdata") or ""):
                await self._sleep(self._login_monitor_interval())
                continue
            try:
                await self._check_login_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 监控任务不允许未捕获异常
                self._logger.error("登录状态监控异常: %s", exc, exc_info=True)
            await self._sleep(self._login_monitor_interval())

    async def _check_login_once(self) -> bool:
        """执行一次登录校验并更新监控状态；返回是否通过。"""
        checker = getattr(self.scheduler, "check_login", None)
        if not callable(checker):
            return True
        try:
            info = await checker()
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 校验失败仅告警，不中断监控
            self._login_consecutive_failures += 1
            self._login_last_error = str(exc)
            self._logger.warning("B 站登录状态校验失败: %s", exc)
            await self._maybe_notify_login_failure()
            return False
        if not info:
            self._login_consecutive_failures += 1
            self._login_last_error = "登录校验返回空"
            self._logger.warning("B 站登录状态校验返回空")
            await self._maybe_notify_login_failure()
            return False
        self._login_consecutive_failures = 0
        self._login_last_error = None
        self._login_last_ok_at = util.now_iso()
        name = info.get("uname") or info.get("mid") or "已登录"
        self._logger.info("B 站登录校验通过：%s", name)
        return True

    async def _maybe_notify_login_failure(self) -> None:
        """连续失败次数恰达阈值时向 notify_session 发送一次告警。"""
        threshold = self._login_monitor_threshold()
        if self._login_consecutive_failures != threshold:
            return
        text = (
            f"B站登录状态连续 {threshold} 次校验失败，请检查 Cookie 是否过期"
            f"（最近错误：{self._login_last_error or '未知'}）"
        )
        await self._send_login_alert(text)

    async def _send_login_alert(self, text: str) -> bool:
        """向 ``login_monitor.notify_session`` 发送告警；未配置会话时仅记日志。"""
        session = str(self._login_monitor_cfg().get("notify_session") or "").strip()
        if not session:
            self._logger.warning("登录状态告警（未配置通知会话）: %s", text)
            return False
        chain = push.build_chain("alert", {"content": text})
        try:
            ok = bool(await self.context.send_message(session, chain))
        except asyncio.CancelledError:
            raise
        except Exception as exc:  # noqa: BLE001 - 告警发送失败不中断监控
            self._logger.warning("登录状态告警发送失败（session=%s）: %s", session, exc)
            return False
        if ok:
            self._logger.info("登录状态告警已发送至 %s", session)
        else:
            self._logger.warning("登录状态告警发送失败（session=%s 不可达）", session)
        return ok

    def login_status(self) -> dict[str, Any]:
        """返回登录校验状态（供 WebUI ``/api/login-status`` 展示）。

        Returns:
            ``{"last_ok_at", "consecutive_failures", "last_error"}``。
        """
        return {
            "last_ok_at": self._login_last_ok_at,
            "consecutive_failures": self._login_consecutive_failures,
            "last_error": self._login_last_error,
        }

    def current_subscriptions(self) -> list[Subscription]:
        """返回调度器当前订阅快照（热重建后保持最新）。

        Returns:
            当前 :class:`Subscription` 列表；scheduler 不提供该访问器时返回
            空列表（离线测试注入的 fake 组件场景）。
        """
        getter = getattr(self.scheduler, "get_subscriptions", None)
        if callable(getter):
            return list(getter())
        return []

    def _log_subscriptions(self) -> None:
        """把当前订阅清单打印到 AstrBot 控制台（启动时调用一次）。

        推送开关摘要由 ``Scheduler._apply_poll_settings`` 在初始化与配置变更
        重建时打印（变更才打），此处不再重复。
        """
        subs = self.current_subscriptions()
        if not subs:
            self._logger.info("当前没有有效订阅（subscriptions 为空或全部被过滤）。")
            return
        self._logger.info("已加载 %d 条 B站订阅：", len(subs))
        for sub in subs:
            state = "启用" if sub.enabled else "禁用"
            targets = "、".join(sub.push_session_ids) or "(无)"
            if sub.type == "collection":
                self._logger.info(
                    "  [合集] %s uid=%s list_id=%s series_type=%s 间隔=%ss %s → %s",
                    sub.name,
                    sub.uid,
                    sub.list_id,
                    sub.series_type,
                    sub.poll_interval_sec,
                    state,
                    targets,
                )
            else:
                self._logger.info(
                    "  [%s] %s uid=%s 间隔=%ss %s → %s",
                    util.type_label(sub.type),
                    sub.name,
                    sub.uid,
                    sub.poll_interval_sec,
                    state,
                    targets,
                )

    async def terminate(self) -> None:
        """逆序关停（幂等）；``asyncio.CancelledError`` 不捕获、自然重抛。

        顺序：置 ``_closing`` → 先停登录监控 → config watcher（阻止新重建）
        → 重建锁内停 scheduler（3 轮询任务 + 维护任务）→ WebUI 释放端口
        → 关库。
        """
        self._closing = True
        login_task, self._login_monitor_task = self._login_monitor_task, None
        if login_task is not None and not login_task.done():
            login_task.cancel()
            await asyncio.gather(login_task, return_exceptions=True)
        await self.reloader.shutdown()
        async with self.reloader.lock:
            await self.scheduler.stop()
        if self.webui is not None:
            await self.webui.stop()
        await self.db.close()

    # ------------------------------------------------------------------
    # WebUI 装配
    # ------------------------------------------------------------------

    @staticmethod
    def _config_raw(config: Any) -> dict[str, Any]:
        """返回配置的 dict 快照；兼容 dict 与属性访问两种形态（同 WebUIServer）。"""
        if isinstance(config, dict):
            return dict(config)
        return {
            key: getattr(config, key, {})
            for key in ("credential", "poll", "webui", "login_monitor", "subscriptions")
        }

    def _webui_config(self) -> dict[str, Any]:
        """返回 webui 配置组的浅拷贝（缺失时为空 dict）。"""
        raw = self._config_raw(self.config)
        webui = raw.get("webui")
        return dict(webui) if isinstance(webui, dict) else {}

    def _webui_enabled(self) -> bool:
        """是否启用 WebUI（schema 缺省 true；字符串 ``"false"`` 不再误判为开）。"""
        return coerce_bool(self._webui_config().get("enabled"), True)

    def _webui_addr(self) -> tuple[str, int]:
        """解析 WebUI 监听地址；非法 port 回退 schema 缺省 8765。"""
        cfg = self._webui_config()
        host = str(cfg.get("host", "127.0.0.1") or "127.0.0.1")
        try:
            port = int(cfg.get("port", 8765))
        except (TypeError, ValueError):
            port = 8765
        return host, port

    def _build_webui(self) -> Any:
        """按 todo 11 契约装配 WebUIServer（重建回调/保存/试推注入）。"""
        return WebUIServer(
            config=self.config,
            request_rebuild=self.reloader.request_rebuild,
            status_provider=lambda: self.scheduler.status,
            config_status_provider=self.reloader.config_status,
            login_status_provider=self.login_status,
            logger=self._logger,
            save_config=lambda cfg: self.config.save_config_async(cfg),
            config_lock=self.reloader.lock,
            build_chain=push.build_chain,
            send_to=self.context.send_message,
        )
