"""AstrBot B站监控插件入口。

监控B站UP主直播开播/下播、动态更新与合集视频更新，自动推送到指定会话。

本模块只保留 **AstrBot 运行时接线**：

- :class:`BilibiliMonitor`：插件主类（Star 子类），组合无 AstrBot 依赖的
  :class:`lifecycle.PluginLifecycle`，initialize/terminate 委托给后者；
  并注册 ``/bili``（别名 ``/bl``）平台指令查询当前会话订阅。
- :func:`query_session_subscriptions`：``/bili`` 指令的文本渲染逻辑。

其余职责已拆到独立模块：

- ``config_file.py``：配置文件路径解析 / 安装兜底初始化 / 批量配置读取与合并。
- ``config_reloader.py``：配置热重载器（防抖 + watcher + 状态清理）。
- ``lifecycle.py``：组件装配 / 启停 / 登录状态监控。
- ``util.py``：跨模块公共小工具（logger / 时间 / 取牌 / 标签）。

为兼容既有测试与外部调用，本模块仍从上述模块**重导出**历史公开名字。
"""

from __future__ import annotations

from typing import Any

try:
    from astrbot.api import AstrBotConfig
    from astrbot.api.star import Context, Star
except ImportError:  # pragma: no cover - 离线测试环境无 AstrBot 运行时
    AstrBotConfig = None  # type: ignore[assignment,misc]
    Context = None  # type: ignore[assignment,misc]
    Star = object  # type: ignore[assignment,misc]  # 离线占位基类

try:
    from astrbot.api.event import AstrMessageEvent, MessageEventResult
    from astrbot.api.event import filter as astrbot_filter
except ImportError:  # pragma: no cover - 离线测试环境无 AstrBot 运行时
    AstrMessageEvent = None  # type: ignore[assignment,misc]
    MessageEventResult = None  # type: ignore[assignment,misc]
    astrbot_filter = None  # type: ignore[assignment,misc]

try:
    from . import config_file, config_reloader, lifecycle, util
    from .config import Subscription
except ImportError:  # pragma: no cover - 离线裸模块导入（测试）
    import config_file  # type: ignore[import-not-found]
    import config_reloader  # type: ignore[import-not-found]
    import lifecycle  # type: ignore[import-not-found]
    import util  # type: ignore[import-not-found]
    from config import Subscription  # type: ignore[import-not-found]

# ---------------------------------------------------------------------------
# 历史公开名字重导出（测试 / 外部调用方不受拆分影响）
# ---------------------------------------------------------------------------

ConfigReloader = config_reloader.ConfigReloader
PluginLifecycle = lifecycle.PluginLifecycle
RebuildResult = config_reloader.RebuildResult

_REBUILD_DEBOUNCE_SEC = config_reloader.REBUILD_DEBOUNCE_SEC
_WATCH_INTERVAL_SEC = config_reloader.WATCH_INTERVAL_SEC
_identity_changed = config_reloader._identity_changed

_default_config_path = config_file._default_config_path
_schema_to_defaults = config_file._schema_to_defaults
ensure_config_file = config_file.ensure_config_file
_bundled_config_path = config_file._bundled_config_path
read_config_file = config_file.read_config_file
_deep_merge = config_file._deep_merge

_SUB_TYPE_LABELS = util.SUBSCRIPTION_TYPE_LABELS
_type_label = util.type_label


def _register_command(command_name: str, alias: set[str] | None = None):
    """返回 AstrBot 指令装饰器；离线环境退化为无操作装饰器。

    Args:
        command_name: 指令名（如 ``bili``）。
        alias: 指令别名集合（可选，如 ``{"bl"}``）。

    Returns:
        AstrBot 指令装饰器；离线（无 AstrBot 运行时）返回仅原样返回被装饰
        函数的无操作装饰器，使 :class:`BilibiliMonitor` 可离线导入测试。
    """
    if astrbot_filter is None:

        def _noop(awaitable: Any) -> Any:
            return awaitable

        return _noop
    return astrbot_filter.command(command_name, alias=alias)


def query_session_subscriptions(subs: list[Subscription], session: str) -> str:
    """返回某会话的订阅清单文本（平台指令 ``/bili`` 的渲染逻辑）。

    按 ``push_session_ids`` 过滤出推送目标包含 ``session`` 的订阅，逐条列出
    类型 / 名称 / 标识 / 启用状态 / 轮询间隔。

    Args:
        subs: 当前订阅快照列表。
        session: 会话 ``unified_msg_origin``（``platform:message_type:session_id``）。

    Returns:
        该会话的订阅清单；无订阅时返回友好提示。
    """
    matched = [sub for sub in subs if session in sub.push_session_ids]
    if not matched:
        return f"当前会话 {session} 没有订阅。"
    lines = [f"当前会话 {session} 的订阅："]
    for index, sub in enumerate(matched, 1):
        state = "启用" if sub.enabled else "禁用"
        if sub.type == "collection":
            target = (
                f"uid={sub.uid}，list_id={sub.list_id}，series_type={sub.series_type}"
            )
        else:
            target = f"uid={sub.uid}"
        lines.append(
            f"{index}. [{util.type_label(sub.type)}] {sub.name}"
            f"（{target}，{state}，间隔 {sub.poll_interval_sec}s）"
        )
    return "\n".join(lines)


class BilibiliMonitor(Star):
    """B站监控插件主类（todo 1 骨架 + todo 13 热重载 + todo 14 生命周期）。

    ``initialize`` / ``terminate`` 委托给无 AstrBot 依赖的
    :class:`PluginLifecycle`（真实组件经 ``PluginLifecycle.create`` 装配）。
    """

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._lifecycle: lifecycle.PluginLifecycle | None = None

    async def initialize(self) -> None:
        """插件激活：启动 db / WebUI / scheduler / config watcher 全链路。"""
        self._lifecycle = lifecycle.PluginLifecycle.create(
            config=self.config, context=self.context, logger=self.logger
        )
        await self._lifecycle.initialize()

    async def terminate(self) -> None:
        """插件停用：逆序关停全部组件（幂等，可重复调用）。"""
        instance, self._lifecycle = self._lifecycle, None
        if instance is not None:
            await instance.terminate()

    def _current_subscriptions(self) -> list[Subscription]:
        """返回当前订阅快照（供平台指令查询）。"""
        instance = self._lifecycle
        if instance is None:
            return []
        return instance.current_subscriptions()

    @_register_command("bili", alias={"bl"})
    async def bili(self, event: AstrMessageEvent) -> None:
        """查询当前会话的 B站订阅列表（/bili 或 /bl）。"""
        session = event.unified_msg_origin
        text = query_session_subscriptions(self._current_subscriptions(), session)
        event.set_result(MessageEventResult().message(text).use_t2i(False))
