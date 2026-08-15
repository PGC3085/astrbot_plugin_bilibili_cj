"""AstrBot B站监控插件。

监控B站UP主直播开播/下播、动态更新与合集视频更新，自动推送到指定会话。

本模块包含三部分：

- :class:`ConfigReloader`：**无 AstrBot 依赖**的配置热重载器（计划 todo 13）——
  单一共享 200ms 防抖窗口 + 串行化重建（同一时刻至多一个重建在跑）、三态
  返回（``parse-failed`` / ``no-op`` / ``rebuilt``）、磁盘重读比对、身份变更
  状态清理、save-then-swap 持久化，以及独立的 5s 配置 watcher 任务。
- :class:`PluginLifecycle`：**无 AstrBot 依赖**的生命周期接线（计划 todo 14）——
  依赖全部注入（config/context/db/scheduler/reloader/webui），``create()``
  工厂按真实组件装配；``initialize()`` 按序启动 db → WebUI → scheduler
  （3 轮询 + 1 维护）→ config watcher；``terminate()`` 逆序关停（先 watcher
  阻止新重建 → 重建锁内停 scheduler → WebUI 释放端口 → 关库），
  ``asyncio.CancelledError`` 全部自然重抛。
- :class:`BilibiliMonitor`：插件主类（Star 子类，需 AstrBot 运行时），组合
  :class:`ConfigReloader` 与 :class:`PluginLifecycle`，initialize/terminate
  委托给后者；并注册 ``/bili``（别名 ``/bl``）平台指令查询当前会话订阅。

除三部分外，本模块还提供：``ensure_config_file()``（安装时按
``_conf_schema.json`` 默认值兜底初始化配置文件）、``query_session_subscriptions()``
（指令查询渲染）、启动时在 AstrBot 控制台打印订阅清单。

离线测试环境无 AstrBot 运行时：模块顶部对 astrbot 导入做保护性 fallback
（``Star`` 退化为 ``object``），使 :class:`ConfigReloader` 与
:class:`PluginLifecycle` 可被直接导入测试。
"""

from __future__ import annotations

import asyncio
import json
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Literal

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
    from .config import Subscription, coerce_bool, normalize
except ImportError:  # pragma: no cover - 离线裸模块导入（测试）
    from config import Subscription, coerce_bool, normalize  # type: ignore[import-not-found]

try:
    from . import push
    from .db import Database
    from .scheduler import Scheduler
    from .webui.server import WebUIServer
except ImportError:  # pragma: no cover - 离线裸模块导入（测试）
    import push  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from scheduler import Scheduler  # type: ignore[import-not-found]
    from webui.server import WebUIServer  # type: ignore[import-not-found]

#: 重建防抖窗口（秒）：窗口内新请求顺延/合并，到期后合并为一次重建。
_REBUILD_DEBOUNCE_SEC: float = 0.2
#: 配置 watcher 轮询间隔（秒）。
_WATCH_INTERVAL_SEC: float = 5.0
#: 插件配置文件文件名（AstrBot 约定：``<plugin 根目录名>_config.json``）。
_CONFIG_FILE_NAME: str = "astrbot_plugin_bilibili_cj_config.json"
#: 插件目录内的批量配置文件名：存在时启动读入并合并到 AstrBot 配置（便于大规模设置）。
_BUNDLED_CONFIG_NAME: str = "config.json"

#: 订阅类型中文标签（启动日志与平台指令展示用）。
_SUB_TYPE_LABELS: dict[str, str] = {
    "live": "直播",
    "dynamic": "动态",
    "collection": "合集",
}

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

#: ``request_rebuild`` 的三态返回：解析失败 / 配置一致未重建 / 已实际重建。
RebuildResult = Literal["parse-failed", "no-op", "rebuilt"]

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


def _type_label(sub_type: str) -> str:
    """返回订阅类型的中文标签，未知类型原样返回。"""
    return _SUB_TYPE_LABELS.get(sub_type, sub_type)


def _now_iso() -> str:
    """当前 UTC 时间的 ISO-8601 字符串（前端按 UTC+8 展示）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


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


def _default_config_path() -> Path:
    """解析生产配置文件路径：``<data>/config/astrbot_plugin_bilibili_cj_config.json``。

    AstrBot 将插件配置存放在 ``get_astrbot_config_path()``（即
    ``<data>/config/``，见 ``star_manager.py`` 的 ``f"{root_dir_name}_config.json"``）；
    AstrBot 不可导入（离线）时回退到相对 ``./data/config/...``。
    """
    try:
        from astrbot.core.utils.astrbot_path import get_astrbot_config_path
    except ImportError:
        _get_logger().warning(
            "get_astrbot_config_path unavailable; using ./data/config/%s",
            _CONFIG_FILE_NAME,
        )
        return Path("data") / "config" / _CONFIG_FILE_NAME
    return Path(get_astrbot_config_path()) / _CONFIG_FILE_NAME


#: ``_conf_schema.json`` 各类型缺省 ``default`` 字段时的回退值（与 AstrBotConfig 同语义）。
_SCHEMA_DEFAULT_MAP: dict[str, Any] = {
    "int": 0,
    "float": 0.0,
    "bool": False,
    "string": "",
    "text": "",
    "list": [],
    "file": [],
    "object": {},
    "template_list": [],
    "dict": {},
}


def _schema_to_defaults(schema: dict[str, Any]) -> dict[str, Any]:
    """把 ``_conf_schema.json`` 转为默认配置字典（与 ``AstrBotConfig`` 同语义）。

    ``object`` 递归展开其 ``items``；其余类型取 ``default``，缺失时按类型回退。

    Args:
        schema: 插件配置 schema（``_conf_schema.json`` 内容）。

    Returns:
        由 schema 派生的默认配置字典。
    """
    conf: dict[str, Any] = {}
    for key, spec in schema.items():
        if spec.get("type") == "object":
            conf[key] = _schema_to_defaults(spec.get("items") or {})
        else:
            conf[key] = spec.get("default", _SCHEMA_DEFAULT_MAP.get(spec.get("type")))
    return conf


def ensure_config_file(
    config_path: str | Path | None = None, logger: logging.Logger | None = None
) -> Path:
    """确保插件配置文件存在：安装时缺失则按 schema 默认值初始化（幂等）。

    AstrBot 在插件加载时会依据 ``_conf_schema.json`` 自动创建配置文件；本函数
    作为插件自身的兜底，覆盖手动安装 / 离线等边缘场景——文件已存在时**不改写**
    任何内容。

    Args:
        config_path: 配置文件路径；None 按 AstrBot 约定解析。
        logger: 显式 logger；缺省用插件统一 logger。

    Returns:
        解析后的配置文件路径。
    """
    path = Path(config_path) if config_path is not None else _default_config_path()
    logger = logger if logger is not None else _get_logger()
    if path.is_file():
        return path
    schema_path = Path(__file__).with_name("_conf_schema.json")
    defaults: dict[str, Any] = {}
    if schema_path.is_file():
        try:
            with open(schema_path, "r", encoding="utf-8-sig") as f:
                defaults = _schema_to_defaults(json.load(f))
        except (OSError, ValueError) as exc:
            logger.warning("读取 _conf_schema.json 失败，回退空默认配置: %s", exc)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, "w", encoding="utf-8") as f:
            json.dump(defaults, f, indent=2, ensure_ascii=False)
            f.write("\n")
        logger.info("初始化插件配置文件: %s", path)
    except OSError as exc:
        logger.warning("初始化配置文件 %s 失败: %s", path, exc)
    return path


def _bundled_config_path() -> Path | None:
    """返回插件目录下的批量配置文件路径；不存在返回 None。"""
    path = Path(__file__).with_name(_BUNDLED_CONFIG_NAME)
    return path if path.is_file() else None


def read_config_file(
    path: str | Path, logger: logging.Logger | None = None
) -> dict[str, Any] | None:
    """读取 JSON 配置文件为 dict；失败返回 None（告警但不中断）。

    Args:
        path: 配置文件路径。
        logger: 显式 logger；缺省用插件统一 logger。

    Returns:
        解析出的 dict；文件不可读 / JSON 非法 / 顶层非对象时返回 None。
    """
    logger = logger if logger is not None else _get_logger()
    try:
        with open(path, "r", encoding="utf-8-sig") as f:
            data = json.load(f)
    except (OSError, ValueError) as exc:
        logger.warning("读取配置文件 %s 失败: %s", path, exc)
        return None
    if not isinstance(data, dict):
        logger.warning("配置文件 %s 顶层不是对象，已忽略", path)
        return None
    return data


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> None:
    """就地深度合并：dict 递归合并，其余（含 list）整体覆盖。

    Args:
        base: 被合并的目标字典（就地修改）。
        override: 覆盖来源字典。
    """
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(base.get(key), dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


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
            f"{index}. [{_type_label(sub.type)}] {sub.name}"
            f"（{target}，{state}，间隔 {sub.poll_interval_sec}s）"
        )
    return "\n".join(lines)


class ConfigReloader:
    """配置热重载器：防抖 + 串行化 + 磁盘比对重建（计划 todo 13）。

    重建的唯一入口是 :meth:`request_rebuild`：

    - **防抖**：单一共享 200ms 窗口，窗口内连续请求合并为一次重建；同一时刻
      至多一个重建在跑，重建期间到达的新请求合并到下一个防抖轮次。
    - **三态返回**：``parse-failed``（磁盘读取/JSON 解析/normalize 失败，未动
      当前任务）、``no-op``（读取成功但与 :attr:`_active_config` 快照一致，
      未重建）、``rebuilt``（已实际重建）。
    - **比对基准**：main.py 持有的 ``_active_config`` 完整规范化配置快照
      （subscriptions + credential + poll），不是实时 ``self.config``——
      WebUI 写盘后重建不会误跳过；settings（轮询/凭据）变更同样触发重建。
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
        logger: logging.Logger | None = None,
        debounce_sec: float = _REBUILD_DEBOUNCE_SEC,
        watch_interval_sec: float = _WATCH_INTERVAL_SEC,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._config_path: Path = (
            Path(config_path) if config_path is not None else _default_config_path()
        )
        self._scheduler: Any = scheduler
        self._db: Any = db
        self._status: dict[str, Any] = status if status is not None else {}
        self._retry_counts: dict[str, dict[str, int]] = (
            retry_counts if retry_counts is not None else {}
        )
        self._config_writer: Any = config_writer
        self._logger: logging.Logger = logger if logger is not None else _get_logger()
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


class PluginLifecycle:
    """插件生命周期接线（计划 todo 14）：**无 Star/AstrBot 依赖**。

    依赖全部注入（``config`` / ``context`` / ``db`` / ``scheduler`` /
    ``reloader`` / ``webui``），:class:`BilibiliMonitor` 仅委托本类完成
    ``initialize()`` / ``terminate()``；离线测试可注入 fake 组件全量驱动。

    启动顺序：``db.init()`` → 凭据检查（无 sessdata 告警匿名模式）→ WebUI
    （仅 ``webui.enabled``，端口绑定失败降级禁用不阻断）→
    ``scheduler.start()`` + ``create_maintenance_task()``（独立稳定引用）→
    ``reloader.start_watcher()``。

    关停顺序（全部幂等，``asyncio.CancelledError`` 不捕获自然重抛）：
    置 ``_closing`` → ``reloader.shutdown()``（先 cancel watcher 阻止新重建）
    → 重建锁内 ``scheduler.stop()``（3 轮询取最新引用 + 维护任务取稳定引用）
    → ``webui.stop()``（释放端口 + 移除日志 Handler）→ ``db.close()``。

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
        logger: logging.Logger | None = None,
        webui: Any = None,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self.config = config
        self.context = context
        self._credential_cfg: dict[str, Any] = dict(credential_cfg)
        self.db = db
        self.scheduler = scheduler
        self.reloader = reloader
        self._logger: logging.Logger = logger if logger is not None else _get_logger()
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
        logger: logging.Logger | None = None,
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
        logger = logger if logger is not None else _get_logger()
        # 安装兜底：配置文件缺失时按 schema 默认值初始化（AstrBot 一般已创建）。
        resolved_path = (
            Path(config_path) if config_path is not None else _default_config_path()
        )
        ensure_config_file(resolved_path, logger=logger)
        # 批量配置：插件目录存在 config.json 时读入并深度合并到 AstrBot 配置，
        # 便于大规模设置订阅（合并后落盘，保证面板 / 热重载 / WebUI 一致）。
        bundled = _bundled_config_path()
        if bundled is not None:
            data = read_config_file(bundled, logger)
            if data is not None:
                _deep_merge(config, data)
                saver = getattr(config, "save_config", None)
                if callable(saver):
                    try:
                        saver()
                    except Exception as exc:  # noqa: BLE001 - 落盘失败不阻断启动
                        logger.warning("批量配置落盘失败: %s", exc)
                logger.info("已读入插件目录批量配置文件 %s 并合并到配置", bundled)
        raw = cls._config_raw(config)
        poll = raw.get("poll")
        if isinstance(poll, dict):
            raw["poll"] = dict(poll)  # normalize 就地钳制：勿动持久配置的 poll 组
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
        reloader = ConfigReloader(
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
        """登录校验间隔（秒），钳制到 :data:`_LOGIN_MONITOR_MIN_INTERVAL` 以上。"""
        raw = self._login_monitor_cfg().get("interval_sec")
        try:
            interval = (
                float(raw) if raw is not None else _LOGIN_MONITOR_DEFAULT_INTERVAL
            )
        except (TypeError, ValueError):
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

    def _start_login_monitor(self) -> None:
        """启动登录状态监控任务（幂等；无 sessdata 或已关闭时不启动）。"""
        if not (self._credential_cfg.get("sessdata") or ""):
            return
        if not self._login_monitor_enabled():
            return
        if self._login_monitor_task is None or self._login_monitor_task.done():
            self._login_monitor_task = asyncio.create_task(
                self._login_monitor_loop(), name="bili-login-monitor"
            )

    async def _login_monitor_loop(self) -> None:
        """周期性校验 B 站登录状态；连续失败达阈值时通知指定会话。

        首次进入立即校验一次，之后每 ``interval_sec`` 校验一次；失败累计连续
        次数、达阈值时发送告警；成功后清零并记录通过时间。每轮重读配置，
        关闭监控（``enabled=false``）后自然退出。
        """
        while True:
            if not self._login_monitor_enabled():
                return
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
        self._login_last_ok_at = _now_iso()
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
                    _type_label(sub.type),
                    sub.name,
                    sub.uid,
                    sub.poll_interval_sec,
                    state,
                    targets,
                )

    async def terminate(self) -> None:
        """逆序关停（幂等）；``asyncio.CancelledError`` 不捕获、自然重抛。

        顺序：置 ``_closing`` → 先停 config watcher（阻止新重建）→ 重建锁内
        停 scheduler（3 轮询任务 + 维护任务）→ WebUI 释放端口 → 关库。
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
        """是否启用 WebUI（schema 缺省 true）。"""
        return bool(self._webui_config().get("enabled", False))

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


class BilibiliMonitor(Star):
    """B站监控插件主类（todo 1 骨架 + todo 13 热重载 + todo 14 生命周期）。

    ``initialize`` / ``terminate`` 委托给无 AstrBot 依赖的
    :class:`PluginLifecycle`（真实组件经 ``PluginLifecycle.create`` 装配）。
    """

    def __init__(self, context: Context, config: AstrBotConfig) -> None:
        super().__init__(context, config)
        self.config = config
        self._lifecycle: PluginLifecycle | None = None

    async def initialize(self) -> None:
        """插件激活：启动 db / WebUI / scheduler / config watcher 全链路。"""
        self._lifecycle = PluginLifecycle.create(
            config=self.config, context=self.context, logger=self.logger
        )
        await self._lifecycle.initialize()

    async def terminate(self) -> None:
        """插件停用：逆序关停全部组件（幂等，可重复调用）。"""
        lifecycle, self._lifecycle = self._lifecycle, None
        if lifecycle is not None:
            await lifecycle.terminate()

    def _current_subscriptions(self) -> list[Subscription]:
        """返回当前订阅快照（供平台指令查询）。"""
        lifecycle = self._lifecycle
        if lifecycle is None:
            return []
        return lifecycle.current_subscriptions()

    @_register_command("bili", alias={"bl"})
    async def bili(self, event: AstrMessageEvent) -> None:
        """查询当前会话的 B站订阅列表（/bili 或 /bl）。"""
        session = event.unified_msg_origin
        text = query_session_subscriptions(self._current_subscriptions(), session)
        event.set_result(MessageEventResult().message(text).use_t2i(False))
