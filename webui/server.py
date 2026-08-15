"""独立 WebUI 后端（计划 todo 11）：aiohttp 静态路由 + JSON API + 鉴权 + 生命周期。

职责：

- 静态路由：``/`` → ``webui/index.html``，``/assets/{path}`` → ``webui/`` 下静态文件
  （前端由 todo 12 提供，未生成时 404）。
- JSON API（全部经 ``Authorization: Bearer <token>`` 鉴权，比对用
  ``hmac.compare_digest``，无 token/错 token 一律 401）：
  - ``GET/POST /api/subscriptions``：整表读写。写前经 ``config.normalize`` 校验
    并保留/分配稳定 ``id``；**任一条目被拒则整体 400 拒绝、不部分落盘**
    （r21/r22 整表语义），400 响应携带被拒条目（index + reason）与 errors。
  - ``POST /api/subscriptions/item``：单条新增/按 id 替换（即时保存），返回
    规范化后的完整订阅列表。
  - ``DELETE /api/subscriptions/{sub_id}``：删除单条订阅（即时保存），返回剩余
    列表；不存在时 404。
  - ``GET /api/status``：调度器 status dict（SimpleNamespace 各字段转 JSON）。
  - ``GET /api/config-status``：配置文件健康状态（``path/ok/last_error``），
    供 WebUI 展示配置读取失败原因，避免控制台日志刷屏。
  - ``GET /api/login-status``：B 站登录校验状态（``last_ok_at/consecutive_failures/
    last_error``），供 WebUI 顶栏展示最近登录校验通过时间。
  - ``GET/POST /api/settings``：credential/poll/webui 分组读写；webui.host/port/
    enabled 变更仅插件重载后生效（README 注明），此处仅持久化。
  - ``POST /api/test-push``：``{session, message}`` 单会话试推，或
    ``{sessions: [...], message}`` 批量试推（返回逐会话结果）；非法会话 400。
  - ``GET /api/logs?tail=N``：有界环形缓冲（deque maxlen=500）最近 N 条日志。
- 生命周期：``start(host, port)`` 幂等（端口被占 → cleanup + error + disabled，
  绝不静默失败）；``stop()`` 幂等（runner.cleanup 释放端口 + 移除日志 Handler）。
- 首启 token 为空：``start()`` 自动生成 uuid4 hex，经配置写锁持久化，
  并 ``logger.info`` 打印一次。
- 配置写锁：与 main.py ``request_rebuild`` 的重建锁是**同一把** ``asyncio.Lock``
  （由 main.py 注入）；锁范围仅包 normalize+save_config，**释放后再调
  ``request_rebuild(clear_disabled=True)``**（request_rebuild 自行取锁重读文件，
  不得跨其持锁否则非重入死锁）。

单文件为计划强制（todo 11 产出仅 ``webui/server.py``），SIZE_OK。
"""

from __future__ import annotations

import asyncio
import hmac
import inspect
import logging
import math
import time
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Awaitable, Callable
from uuid import uuid4

from aiohttp import web

try:
    from ..config import (
        SUBSCRIPTION_TYPES,
        _REQUIRED_FIELDS,
        _normalize_sessions,
        _to_int_or_none,
        normalize,
        validate_session,
    )
    from ..push import format_event_time
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import (  # type: ignore[import-not-found]
        SUBSCRIPTION_TYPES,
        _REQUIRED_FIELDS,
        _normalize_sessions,
        _to_int_or_none,
        normalize,
        validate_session,
    )
    from push import format_event_time  # type: ignore[import-not-found]

#: /api/logs 环形缓冲容量（条）。
_LOG_RING_MAX = 500
#: /api/logs 未指定 tail 时的默认条数。
_LOG_TAIL_DEFAULT = 100

_logger: logging.Logger | None = None


def _get_logger() -> logging.Logger:
    """返回插件统一 logger；离线环境回退 stdlib logger。"""
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger("astrbot_plugin_bilibili_cj")
        else:
            _logger = astrbot_logger
    return _logger


def _auth_middleware_factory(get_token: Callable[[], str]) -> Any:
    """构造 Bearer token 鉴权中间件，守护全部 ``/api/*`` 路由。

    Args:
        get_token: 返回当前生效访问令牌的可调用对象。

    Returns:
        aiohttp 中间件；未带 ``Authorization: Bearer <token>`` 或令牌不符时
        返回 401 ``{"error": "unauthorized"}``，静态路由不受影响。
    """

    @web.middleware
    async def _auth(
        request: web.Request,
        handler: Callable[[web.Request], Awaitable[web.StreamResponse]],
    ) -> web.StreamResponse:
        if request.path.startswith("/api/"):
            token = get_token()
            header = request.headers.get("Authorization", "")
            supplied = header[len("Bearer ") :] if header.startswith("Bearer ") else ""
            if not token or not hmac.compare_digest(supplied, token):
                return web.json_response({"error": "unauthorized"}, status=401)
        return await handler(request)

    return _auth


class _RingBufferHandler(logging.Handler):
    """把格式化后的 LogRecord 转发给回调的轻量 logging.Handler（/api/logs 数据源）。

    Args:
        sink: 接收 LogRecord 的回调（``WebUIServer.feed_log``）。
    """

    def __init__(self, sink: Callable[[logging.LogRecord], None]) -> None:
        super().__init__()
        self._sink = sink

    def emit(self, record: logging.LogRecord) -> None:
        try:
            self._sink(record)
        except Exception:  # noqa: BLE001 - logging 回调不允许向上抛
            self.handleError(record)


class WebUIServer:
    """独立 WebUI 的 aiohttp 后端（静态 + JSON API + Bearer 鉴权 + 生命周期）。

    Args:
        config: 持久化 AstrBotConfig 实例（dict 子类）或 dict-like 对象
            （支持 ``[]`` / ``.get()`` / ``getattr``），承载
            credential/poll/webui/subscriptions 四个配置组。
        request_rebuild: main.py 注入的重建回调 ``(clear_disabled: bool) ->
            "parse-failed"|"no-op"|"rebuilt"``，可为 async 或 sync（重建锁由
            main.py 持有；本类在配置写锁释放后调用，绝不跨锁持有）。
        status_provider: 返回调度器 status dict（sub_id -> 可变状态对象）的
            可调用对象。
        logger: 显式 logger；缺省用插件统一 logger。
        token: 显式访问令牌；为空时回退读 ``config.webui.token``，仍为空则
            ``start()`` 时自动生成并持久化、日志打印一次。
        save_config: 配置写盘回调 ``(config) -> Awaitable[None]``（main.py
            注入 ``lambda cfg: self.config.save_config_async()``）；缺省尝试
            ``config.save_config_async``。
        config_lock: 配置写锁；与 main.py ``request_rebuild`` 的重建锁为同一把
            ``asyncio.Lock``（main.py 注入），缺省自建。
        build_chain: 试推链构造器（与 ``push.build_chain`` 同签名）；缺省纯文本。
        send_to: 试推发送回调 ``(session, chain) -> Awaitable[bool]``（main.py
            注入 ``context.send_message`` 语义）；缺省记告警并返回 False。
    """

    def __init__(
        self,
        config: Any,
        request_rebuild: Callable[[bool], str | Awaitable[str]],
        status_provider: Callable[[], dict[str, Any]],
        logger: logging.Logger | None = None,
        token: str = "",
        save_config: Callable[[dict[str, Any]], Awaitable[None]] | None = None,
        config_lock: asyncio.Lock | None = None,
        build_chain: Callable[[str, dict[str, Any]], Any] | None = None,
        send_to: Callable[[str, Any], Awaitable[bool]] | None = None,
        config_status_provider: Callable[[], dict[str, Any]] | None = None,
        login_status_provider: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._config = config
        self._request_rebuild = request_rebuild
        self._status_provider = status_provider
        self._logger = logger if logger is not None else _get_logger()
        self._save_config = save_config
        self._config_lock = config_lock if config_lock is not None else asyncio.Lock()
        self._build_chain = (
            build_chain if build_chain is not None else self._default_build_chain
        )
        self._send_to = send_to if send_to is not None else self._default_send_to
        self._config_status_provider = (
            config_status_provider
            if config_status_provider is not None
            else self._default_config_status
        )
        self._login_status_provider = (
            login_status_provider
            if login_status_provider is not None
            else self._default_login_status
        )
        self._token = token or str(self._webui_config().get("token", "") or "")
        self._static_root = Path(__file__).resolve().parent
        self._log_buffer: deque[str] = deque(maxlen=_LOG_RING_MAX)
        self._log_formatter = logging.Formatter(
            "%(asctime)s %(levelname)s %(name)s: %(message)s"
        )
        self._log_handler: logging.Handler | None = None
        self._log_target: logging.Logger | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._start_task: asyncio.Task[Any] | None = None
        self._host: str = ""
        self._port: int = 0
        #: 服务是否已成功启动（端口绑定失败时为 False）。
        self.enabled = False
        self._app = web.Application(
            middlewares=[_auth_middleware_factory(self._get_token)]
        )
        self._app.router.add_get("/", self._index)
        self._app.router.add_get("/assets/{path:.*}", self._assets)
        self._app.router.add_get("/api/subscriptions", self._api_subscriptions_get)
        self._app.router.add_post("/api/subscriptions", self._api_subscriptions_post)
        self._app.router.add_post(
            "/api/subscriptions/item", self._api_subscriptions_item_post
        )
        self._app.router.add_delete(
            "/api/subscriptions/{sub_id}", self._api_subscriptions_delete
        )
        self._app.router.add_get("/api/status", self._api_status_get)
        self._app.router.add_get("/api/config-status", self._api_config_status_get)
        self._app.router.add_get("/api/login-status", self._api_login_status_get)
        self._app.router.add_get("/api/settings", self._api_settings_get)
        self._app.router.add_post("/api/settings", self._api_settings_post)
        self._app.router.add_post("/api/test-push", self._api_test_push_post)
        self._app.router.add_get("/api/logs", self._api_logs_get)

    @property
    def app(self) -> web.Application:
        """aiohttp Application（TestServer / 生命周期使用）。"""
        return self._app

    # ------------------------------------------------------------------
    # 配置访问（兼容 AstrBotConfig dict 形态与属性形态）
    # ------------------------------------------------------------------

    def _config_get(self, key: str, default: Any = None) -> Any:
        """读取配置项；兼容 dict 与属性访问两种 config 形态。"""
        config = self._config
        if isinstance(config, dict):
            return config.get(key, default)
        return getattr(config, key, default)

    def _config_set(self, key: str, value: Any) -> None:
        """写入配置项（内存）；持久化由 save_config 完成。"""
        config = self._config
        if isinstance(config, dict):
            config[key] = value
        else:
            setattr(config, key, value)

    def _webui_config(self) -> dict[str, Any]:
        """返回 webui 配置组的浅拷贝（缺失时为空 dict，不改原配置）。"""
        raw = self._config_get("webui", None)
        return dict(raw) if isinstance(raw, dict) else {}

    def _config_snapshot(self) -> dict[str, Any]:
        """返回配置浅拷贝（poll 组深拷贝，供 normalize 只读校验、零副作用）。"""
        if isinstance(self._config, dict):
            snapshot: dict[str, Any] = dict(self._config)
        else:
            snapshot = {
                key: getattr(self._config, key, {})
                for key in ("credential", "poll", "webui", "subscriptions")
            }
        poll = snapshot.get("poll")
        if isinstance(poll, dict):
            snapshot["poll"] = dict(poll)
        return snapshot

    @staticmethod
    async def _await_maybe(value: Any) -> Any:
        """等待可等待对象；普通返回值原样返回（兼容 sync 测试回调）。"""
        if inspect.isawaitable(value):
            return await value
        return value

    async def _call_save_config(self) -> None:
        """持久化配置：优先注入的 save_config，其次 config.save_config_async。"""
        if self._save_config is not None:
            await self._await_maybe(self._save_config(self._config))
            return
        saver = getattr(self._config, "save_config_async", None)
        if callable(saver):
            await self._await_maybe(saver())
            return
        self._logger.warning("WebUI 未注入 save_config，配置修改未持久化")

    async def _rebuild(self, clear_disabled: bool) -> str:
        """调用注入的 request_rebuild（锁外）；返回三态结果字符串。"""
        return await self._await_maybe(self._request_rebuild(clear_disabled))

    # ------------------------------------------------------------------
    # 鉴权 / 请求辅助
    # ------------------------------------------------------------------

    def _get_token(self) -> str:
        """返回当前生效的访问令牌（供鉴权中间件调用）。"""
        return self._token

    async def _read_json_body(self, request: web.Request) -> dict[str, Any] | None:
        """解析请求 JSON 对象；请求体非法或非对象时返回 None。"""
        try:
            data = await request.json()
        except (ValueError, web.HTTPException):
            return None
        return data if isinstance(data, dict) else None

    def _bad_request(self, message: str, **extra: Any) -> web.Response:
        """构造 400 JSON 响应（错误信息 + 可选附加字段）。"""
        return web.json_response({"error": message, **extra}, status=400)

    def _reject_reason(self, raw: Any) -> str | None:
        """镜像 config.normalize 的拒收条件，返回原因字符串；合法返回 None。

        与 normalize 的判定共用同一批私有校验函数（_REQUIRED_FIELDS /
        _to_int_or_none / _normalize_sessions），保证 400 响应中被拒条目
        与整表 normalize 的结果一致（镜像一致性由测试锁定）。
        """
        if not isinstance(raw, dict):
            return "条目不是 JSON 对象"
        sub_type = raw.get("type")
        if sub_type not in SUBSCRIPTION_TYPES:
            return f"type={sub_type!r} 非法（可选 live/dynamic/collection）"
        missing = [
            field for field in _REQUIRED_FIELDS[sub_type] if raw.get(field) is None
        ]
        if missing:
            return f"缺少必填字段 {missing}"
        if _to_int_or_none(raw.get("uid")) is None:
            return "uid 必须为数字"
        if sub_type == "collection" and (
            _to_int_or_none(raw.get("list_id")) is None
            or _to_int_or_none(raw.get("series_type")) is None
        ):
            return "list_id/series_type 必须为数字"
        sessions = raw.get("push_session_ids")
        if not isinstance(sessions, list):
            return "push_session_ids 必须为字符串数组"
        if not sessions:
            return "push_session_ids 不能为空"
        if not _normalize_sessions(sessions):
            return "push_session_ids 中的会话格式均非法（应为 platform:message_type:session_id）"
        poll_interval = raw.get("poll_interval_sec")
        if poll_interval is not None and (
            not isinstance(poll_interval, (int, float))
            or isinstance(poll_interval, bool)
        ):
            return "poll_interval_sec 必须为数字"
        return None

    # ------------------------------------------------------------------
    # 静态路由
    # ------------------------------------------------------------------

    async def _index(self, request: web.Request) -> web.FileResponse:
        """GET /：返回 webui/index.html（前端由 todo 12 提供，未生成时 404）。"""
        del request
        index = self._static_root / "index.html"
        if not index.is_file():
            raise web.HTTPNotFound(text="webui/index.html 尚未生成")
        return web.FileResponse(index)

    async def _assets(self, request: web.Request) -> web.FileResponse:
        """GET /assets/{path}：返回 webui/ 下静态文件（防路径穿越）。"""
        rel = request.match_info.get("path", "")
        target = (self._static_root / rel).resolve()
        if not target.is_relative_to(self._static_root) or not target.is_file():
            raise web.HTTPNotFound()
        return web.FileResponse(target)

    # ------------------------------------------------------------------
    # JSON API：订阅
    # ------------------------------------------------------------------

    async def _api_subscriptions_get(self, request: web.Request) -> web.Response:
        """GET /api/subscriptions：返回规范化订阅列表（id 保留）。"""
        del request
        subs = normalize(self._config_snapshot())
        return web.json_response({"subscriptions": [s.to_dict() for s in subs]})

    async def _api_subscriptions_post(self, request: web.Request) -> web.Response:
        """POST /api/subscriptions：整表写回，任一条目被拒则整体 400。

        校验经 config.normalize（保留/分配稳定 id）；被拒条目以
        ``{index, reason}`` 形式在 400 响应中标识（不静默 200，避免用户
        提交的订阅凭空消失）。全部合法才落盘：**读取-规范化-写盘整体在配置
        写锁内**（防并发整表保存互相覆盖），**释放锁后**调 ``request_rebuild(True)``。
        """
        data = await self._read_json_body(request)
        if data is None:
            return self._bad_request("请求体必须为 JSON 对象")
        raw_subs = data.get("subscriptions")
        if not isinstance(raw_subs, list):
            return self._bad_request("subscriptions 必须为数组")
        async with self._config_lock:
            snapshot = self._config_snapshot()
            snapshot["subscriptions"] = raw_subs
            valid = normalize(snapshot)
            if len(valid) != len(raw_subs):
                rejected = [
                    {"index": index, "reason": reason}
                    for index, raw in enumerate(raw_subs)
                    if (reason := self._reject_reason(raw)) is not None
                ]
                errors = [
                    f"订阅 #{item['index']}: {item['reason']}" for item in rejected
                ]
                self._logger.warning(
                    "WebUI 订阅整表保存被拒 %d/%d 条，整体拒绝未写入: %s",
                    len(rejected),
                    len(raw_subs),
                    errors,
                )
                return web.json_response(
                    {
                        "ok": False,
                        "error": "订阅列表含非法条目，整体拒绝（未写入）",
                        "rejected": rejected,
                        "errors": errors,
                    },
                    status=400,
                )
            self._config_set("subscriptions", [sub.to_dict() for sub in valid])
            await self._call_save_config()
        result = await self._rebuild(True)
        return web.json_response({"ok": True, "count": len(valid), "rebuild": result})

    async def _api_subscriptions_item_post(self, request: web.Request) -> web.Response:
        """POST /api/subscriptions/item：单条新增或按 id 替换，立即落盘 + 重建。

        校验经 ``_reject_reason``（与 normalize 判定一致）；读取-规范化-写盘
        整体在配置写锁内，避免并发 upsert 互相覆盖；返回规范化后的完整订阅
        列表，前端可直接替换本地状态，无需二次整表刷新。
        """
        data = await self._read_json_body(request)
        if data is None:
            return self._bad_request("请求体必须为 JSON 对象")
        raw_item = data.get("subscription")
        if not isinstance(raw_item, dict):
            return self._bad_request("subscription 必须为对象")
        reason = self._reject_reason(raw_item)
        if reason is not None:
            return self._bad_request(reason)
        raw_id = raw_item.get("id")
        sub_id = raw_id if isinstance(raw_id, str) and raw_id else None
        async with self._config_lock:
            current = normalize(self._config_snapshot())
            next_list: list[Any] = []
            replaced = False
            for sub in current:
                if sub_id is not None and sub.id == sub_id:
                    next_list.append(raw_item)
                    replaced = True
                else:
                    next_list.append(sub.to_dict())
            if not replaced:
                next_list.append(raw_item)
            snapshot = self._config_snapshot()
            snapshot["subscriptions"] = next_list
            valid = normalize(snapshot)
            if len(valid) != len(next_list):
                return self._bad_request(
                    "订阅项校验未通过（如平台不支持主动消息），未写入"
                )
            self._config_set("subscriptions", [sub.to_dict() for sub in valid])
            await self._call_save_config()
        result = await self._rebuild(True)
        return web.json_response(
            {
                "ok": True,
                "rebuild": result,
                "subscriptions": [sub.to_dict() for sub in valid],
            }
        )

    async def _api_subscriptions_delete(self, request: web.Request) -> web.Response:
        """DELETE /api/subscriptions/{sub_id}：删除单条订阅，立即落盘 + 重建。

        读取-删除-写盘整体在配置写锁内，避免与并发写请求互相覆盖。
        """
        sub_id = request.match_info.get("sub_id", "")
        async with self._config_lock:
            current = normalize(self._config_snapshot())
            remaining = [sub for sub in current if sub.id != sub_id]
            if len(remaining) == len(current):
                return web.json_response(
                    {"ok": False, "error": "订阅不存在"}, status=404
                )
            self._config_set("subscriptions", [sub.to_dict() for sub in remaining])
            await self._call_save_config()
        result = await self._rebuild(True)
        return web.json_response(
            {
                "ok": True,
                "count": len(remaining),
                "rebuild": result,
                "subscriptions": [sub.to_dict() for sub in remaining],
            }
        )

    # ------------------------------------------------------------------
    # JSON API：状态 / 设置 / 试推 / 日志
    # ------------------------------------------------------------------

    async def _api_status_get(self, request: web.Request) -> web.Response:
        """GET /api/status：返回调度器 status dict（按 sub_id）。"""
        del request
        return web.json_response(self._status_to_jsonable())

    async def _api_config_status_get(self, request: web.Request) -> web.Response:
        """GET /api/config-status：返回配置文件健康状态（供 WebUI 展示）。"""
        del request
        return web.json_response(self._config_status_provider())

    @staticmethod
    def _default_config_status() -> dict[str, Any]:
        """未注入 config_status_provider 时的缺省健康状态（视为正常）。"""
        return {"path": "", "ok": True, "last_error": None}

    async def _api_login_status_get(self, request: web.Request) -> web.Response:
        """GET /api/login-status：返回 B 站登录校验状态（供顶栏展示）。"""
        del request
        return web.json_response(self._login_status_provider())

    @staticmethod
    def _default_login_status() -> dict[str, Any]:
        """未注入 login_status_provider 时的缺省登录状态。"""
        return {"last_ok_at": None, "consecutive_failures": 0, "last_error": None}

    def _status_to_jsonable(self) -> dict[str, Any]:
        """把 status dict 转为可 JSON 序列化的 dict（按 sub_id）。"""
        result: dict[str, Any] = {}
        for sub_id, entry in self._status_provider().items():
            if isinstance(entry, SimpleNamespace):
                result[sub_id] = vars(entry)
            else:
                result[sub_id] = {
                    name: getattr(entry, name, None)
                    for name in (
                        "last_poll",
                        "last_error",
                        "error_count",
                        "live_status",
                        "last_push_at",
                        "auto_disabled",
                    )
                }
        return result

    async def _api_settings_get(self, request: web.Request) -> web.Response:
        """GET /api/settings：返回 credential/poll/webui 三个配置组。"""
        del request
        return web.json_response(
            {
                "credential": self._config_get("credential", None) or {},
                "poll": self._config_get("poll", None) or {},
                "webui": self._webui_config(),
            }
        )

    async def _api_settings_post(self, request: web.Request) -> web.Response:
        """POST /api/settings：写回 credential/poll/webui 分组（锁内落盘、锁外重建）。

        写前校验：poll 数值必须为有限且达下限（避免 0/负数/NaN/inf 引发紧循环
        或睡眠异常），webui.port 必须为 1-65535 整数、host 必须为非空字符串。
        webui.host/port/enabled 变更仅插件重载后生效（README 注明），此处仅持久化。
        """
        data = await self._read_json_body(request)
        if data is None:
            return self._bad_request("请求体必须为 JSON 对象")
        merged: dict[str, dict[str, Any]] = {}
        for key in ("credential", "poll", "webui"):
            value = data.get(key)
            if value is None:
                continue
            if not isinstance(value, dict):
                return self._bad_request(f"{key} 必须为对象")
            merged[key] = value
        if not merged:
            return self._bad_request("请求体为空，无可写回的设置")
        poll_value = merged.get("poll")
        if poll_value is not None:
            interval = poll_value.get("global_min_interval_sec")
            if interval is not None:
                try:
                    interval_num = float(interval)
                except (TypeError, ValueError, OverflowError):
                    return self._bad_request("poll.global_min_interval_sec 必须为数字")
                if not math.isfinite(interval_num) or interval_num < 1:
                    return self._bad_request(
                        "poll.global_min_interval_sec 必须为 ≥1 的有限数字"
                    )
            jitter = poll_value.get("poll_jitter_sec")
            if jitter is not None:
                try:
                    jitter_num = float(jitter)
                except (TypeError, ValueError, OverflowError):
                    return self._bad_request("poll.poll_jitter_sec 必须为数字")
                if not math.isfinite(jitter_num) or jitter_num < 0:
                    return self._bad_request(
                        "poll.poll_jitter_sec 必须为 ≥0 的有限数字"
                    )
        webui_value = merged.get("webui")
        if webui_value is not None:
            port = webui_value.get("port")
            if port is not None and (
                not isinstance(port, int)
                or isinstance(port, bool)
                or not 1 <= port <= 65535
            ):
                return self._bad_request("webui.port 必须为 1-65535 的整数")
            host = webui_value.get("host")
            if host is not None and (not isinstance(host, str) or not host.strip()):
                return self._bad_request("webui.host 必须为非空字符串")
        async with self._config_lock:
            for key, value in merged.items():
                current = self._config_get(key, None)
                if isinstance(current, dict):
                    current.update(value)
                else:
                    self._config_set(key, dict(value))
            await self._call_save_config()
        await self._rebuild(True)
        return web.json_response({"ok": True})

    async def _api_test_push_post(self, request: web.Request) -> web.Response:
        """POST /api/test-push：单会话或批量试推，可仿照指定事件类型生成文案。

        body 支持 ``{session, message}`` / ``{sessions: [...], message}``（向后
        兼容，默认 ``dynamic`` 事件类型），或额外带 ``event_type``
        （live_on / live_off / live_title / dynamic / collection）以对应类型的
        真实推送格式生成测试文案。任一非法会话 400，不发送。
        """
        data = await self._read_json_body(request)
        if data is None:
            return self._bad_request("请求体必须为 JSON 对象")
        message = data.get("message")
        if not isinstance(message, str):
            return self._bad_request("message 必须为字符串")
        sessions = self._parse_test_sessions(data)
        if sessions is None:
            return self._bad_request(
                "session（字符串）或 sessions（字符串数组）必须提供其一"
            )
        if not sessions:
            return self._bad_request("sessions 不能为空")
        for session in sessions:
            try:
                validate_session(session)
            except ValueError as exc:
                return web.json_response({"ok": False, "detail": str(exc)}, status=400)
        event_type = data.get("event_type")
        if not isinstance(event_type, str) or not event_type:
            event_type = "dynamic"
        chain = self._build_chain(event_type, self._test_payload(event_type, message))
        results: dict[str, bool] = {}
        for session in sessions:
            try:
                results[session] = bool(await self._send_to(session, chain))
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001 - 单次试推失败不中断批量
                self._logger.warning(
                    "WebUI 测试推送失败（session=%s）: %s", session, exc
                )
                results[session] = False
        all_ok = bool(results) and all(results.values())
        if len(sessions) == 1:
            detail = "推送成功" if all_ok else "推送失败（平台未找到或目标不可达）"
        else:
            success_count = sum(1 for ok in results.values() if ok)
            detail = f"成功 {success_count}/{len(sessions)}"
        return web.json_response({"ok": all_ok, "detail": detail, "results": results})

    @staticmethod
    def _test_payload(event_type: str, message: str) -> dict[str, Any]:
        """按事件类型构造代表性测试载荷（复用真实推送模板渲染）。

        Args:
            event_type: 事件类型（live_on/live_off/live_title/dynamic/collection）。
            message: 用户自定义文案，映射到该类型最相关的内容字段。

        Returns:
            与该事件类型模板占位符匹配的测试载荷；未知类型回退通用 dynamic。
        """
        now = format_event_time(int(time.time()))
        message = message or "测试内容"
        if event_type == "live_on":
            return {
                "name": "测试主播",
                "title": message,
                "area_name": "测试分区",
                "live_start_time": now,
                "url": "https://live.bilibili.com/1",
                "cover": "",
            }
        if event_type == "live_off":
            return {
                "name": "测试主播",
                "duration": 3600,
                "event_time": now,
                "url": "https://live.bilibili.com/1",
            }
        if event_type == "live_title":
            return {
                "name": "测试主播",
                "old_title": "旧标题",
                "new_title": message,
                "event_time": now,
                "url": "https://live.bilibili.com/1",
            }
        if event_type == "dynamic":
            return {
                "name": "测试UP",
                "action": "发布了新动态：",
                "body": message,
                "event_time": now,
                "url": "https://t.bilibili.com/0",
            }
        if event_type == "collection":
            return {
                "name": "测试合集订阅",
                "video_title": message,
                "list_name": "测试合集",
                "publish_time": now,
                "url": "https://www.bilibili.com/video/BV0000",
            }
        return {"name": "测试推送", "action": "发布了新动态：", "body": message}

    def _parse_test_sessions(self, data: dict[str, Any]) -> list[str] | None:
        """解析试推的 ``session`` / ``sessions`` 参数；缺失或类型非法返回 None。

        Args:
            data: 请求体对象。

        Returns:
            会话字符串列表；既无合法 ``session`` 也无合法 ``sessions`` 时返回
            None。
        """
        sessions = data.get("sessions")
        if sessions is not None:
            if not isinstance(sessions, list) or not all(
                isinstance(s, str) for s in sessions
            ):
                return None
            return sessions
        session = data.get("session")
        if isinstance(session, str):
            return [session]
        return None

    async def _api_logs_get(self, request: web.Request) -> web.Response:
        """GET /api/logs?tail=N：返回环形缓冲最近 N 条日志（0<=N<=500）。"""
        raw_tail = request.query.get("tail")
        if raw_tail is None:
            tail = _LOG_TAIL_DEFAULT
        else:
            try:
                tail = int(raw_tail)
            except ValueError:
                return self._bad_request("tail 必须为整数")
        tail = max(0, min(tail, _LOG_RING_MAX))
        entries = [] if tail == 0 else list(self._log_buffer)[-tail:]
        return web.json_response({"logs": entries, "total": len(entries)})

    def feed_log(self, record: logging.LogRecord) -> None:
        """向 /api/logs 环形缓冲追加一条格式化日志（logging.Handler 回调）。"""
        self._log_buffer.append(self._log_formatter.format(record))

    def install_log_handler(self, target: logging.Logger | str | None = None) -> None:
        """把有界日志 Handler 挂到目标 logger 并喂 /api/logs（幂等）。

        Args:
            target: 显式 logger 或 logger 名；缺省离线回退
                ``astrbot_plugin_bilibili_cj``，AstrBot 运行时挂到 astrbot 统一
                logger（main.py initialize 调用，terminate 时由 stop/remove 移除）。
        """
        if self._log_handler is not None:
            return
        handler = _RingBufferHandler(self.feed_log)
        handler.setFormatter(self._log_formatter)
        target_logger = self._resolve_target_logger(target)
        target_logger.addHandler(handler)
        self._log_handler = handler
        self._log_target = target_logger

    def remove_log_handler(self) -> None:
        """移除日志 Handler（幂等）。"""
        if self._log_handler is None:
            return
        if self._log_target is not None:
            self._log_target.removeHandler(self._log_handler)
        self._log_handler = None
        self._log_target = None

    def _resolve_target_logger(
        self, target: logging.Logger | str | None
    ) -> logging.Logger:
        """解析日志 Handler 的挂载目标 logger。"""
        if isinstance(target, logging.Logger):
            return target
        if isinstance(target, str):
            return logging.getLogger(target)
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            return logging.getLogger("astrbot_plugin_bilibili_cj")
        return astrbot_logger

    # ------------------------------------------------------------------
    # 生命周期
    # ------------------------------------------------------------------

    async def _ensure_token(self) -> None:
        """首启 token 为空时生成 uuid4 hex 令牌并持久化，日志打印一次。"""
        if self._token:
            return
        new_token = uuid4().hex
        try:
            async with self._config_lock:
                self._token = new_token
                webui_cfg = self._webui_config()
                webui_cfg["token"] = new_token
                self._config_set("webui", webui_cfg)
                await self._call_save_config()
        except Exception as exc:  # noqa: BLE001 - token 持久化失败不阻断启动
            self._logger.warning("WebUI 令牌持久化失败（本次运行仍生效）: %s", exc)
        self._logger.info(
            "WebUI 首次启动，已自动生成访问令牌（请妥善保存）：%s", new_token
        )

    async def start(self, host: str, port: int) -> None:
        """启动 HTTP 服务（幂等）。

        Runner setup + site bind 在后台任务中执行，``start()`` 等待其完成，
        使调用方（main.py initialize）同步感知绑定结果。绑定失败：cleanup
        runner、``logger.error``、``enabled=False``——绝不静默失败、绝不崩溃。
        """
        if self._runner is not None:
            return
        await self._ensure_token()
        runner = web.AppRunner(self._app, access_log=None)
        self._runner = runner
        self._host = host
        self._port = port
        self._start_task = asyncio.create_task(
            self._serve(host, port), name="bili-webui-serve"
        )
        await self._start_task

    async def _serve(self, host: str, port: int) -> None:
        """绑定 TCP 站点；失败则清理、禁用并记错误，CancelledError 透传。"""
        try:
            runner = self._runner
            if runner is not None:
                await runner.setup()
            site = web.TCPSite(runner, host, port)
            await site.start()
        except asyncio.CancelledError:
            await self._cleanup()
            raise
        except Exception as exc:  # noqa: BLE001 - 端口绑定失败属预期路径
            await self._cleanup()
            self.enabled = False
            self._logger.error(
                "WebUI 启动失败（host=%s port=%s，可能被占用）: %s",
                self._host,
                self._port,
                exc,
            )
        else:
            self._site = site
            self.enabled = True

    async def _cleanup(self) -> None:
        """清理 runner 引用（幂等；cleanup 异常吞掉记日志）。"""
        runner = self._runner
        self._runner = None
        self._site = None
        if runner is not None:
            try:
                await runner.cleanup()
            except Exception:  # noqa: BLE001 - 清理失败不掩盖主流程
                self._logger.exception("WebUI runner cleanup 失败")

    async def stop(self) -> None:
        """停止服务（幂等）：取消未完成的启动任务、清理 runner 释放端口、
        移除日志 Handler。"""
        if self._start_task is not None and not self._start_task.done():
            self._start_task.cancel()
            await asyncio.gather(self._start_task, return_exceptions=True)
        self._start_task = None
        await self._cleanup()
        self.remove_log_handler()
        self.enabled = False

    # ------------------------------------------------------------------
    # 缺省注入回调（main.py 未注入时的降级）
    # ------------------------------------------------------------------

    def _default_build_chain(self, event_type: str, payload: dict[str, Any]) -> Any:
        """缺省试推链构造器：纯文本（无 AstrBot MessageChain 环境）。"""
        del event_type
        content = payload.get("body") or payload.get("content", "")
        return f"【测试推送】{content}"

    async def _default_send_to(self, session: str, chain: Any) -> bool:
        """缺省试推发送器：未注入 send_to 时记告警并返回 False。"""
        del chain
        self._logger.warning(
            "WebUI 未注入 send_to，无法发送测试推送（session=%s）", session
        )
        return False
