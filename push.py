"""Push module: message templates, optional cover image, and per-session delivery.

Consumed by the pollers (todo 6/7/8). Two public entry points:

- :func:`build_chain` turns an event payload into the push content. When the
  AstrBot runtime is available it returns a ``MessageChain`` (text plus an
  optional cover image); when it is not (offline tests / T17 smoke) it returns
  a plain ``str``. Callers pass the result straight to
  ``context.send_message(session, chain)`` without caring which type it is.
- :func:`send` delivers a prebuilt chain to every ``push_session_ids`` target
  of a subscription, returning a per-session success map and recording
  ``last_push_at`` / ``last_error`` into the runtime ``status`` dict.

Offline strategy: the ``astrbot.api`` imports are guarded so this module stays
importable without AstrBot (``ModuleNotFoundError``/``ImportError`` are caught
and the API symbols become ``None``). :func:`build_chain` then degrades to
plain text. Cover-image construction is additionally wrapped so a failing
``Comp.Image.fromURL`` (bad URL / network) degrades to text-only and never
aborts a push.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from string import Formatter
from types import SimpleNamespace
from typing import Any

try:
    from .config import Subscription, validate_session
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription, validate_session  # type: ignore[import-not-found]

try:
    from astrbot.api.message import MessageChain, Comp  # type: ignore[import-not-found]

    _ASTRBOT_AVAILABLE = True
except ImportError:
    try:
        from astrbot.api.event import MessageChain  # type: ignore[import-not-found]
        import astrbot.api.message_components as Comp  # type: ignore[import-not-found]

        _ASTRBOT_AVAILABLE = True
    except ImportError:
        MessageChain = None  # type: ignore[assignment]
        Comp = None  # type: ignore[assignment]
        _ASTRBOT_AVAILABLE = False

#: 事件时间展示时区（固定 UTC+8，中国标准时间，无夏令时）。
_EVENT_TZ = timezone(timedelta(hours=8))

_MAX_TEXT_LEN = 500
"""推送文本的最大长度（字符），超出部分以省略号截断。"""

# 模板占位符即载荷键（``live_on`` 的 ``area_name``/``live_start_time`` 直接
# 作为占位符，渲染结果与任务模板 ``分区：{area}``/``开始：{time}`` 一致）。
# 载荷键与占位符一一对应，缺失键渲染为空字符串，不会抛 ``KeyError``。
_TEMPLATES: dict[str, str] = {
    "live_on": "【B站开播】{name}\n标题：{title}\n分区：{area_name}\n开播时间：{live_start_time}\n链接：{url}",
    "live_off": "【B站下播】{name}\n时长：{duration}\n下播时间：{event_time}\n链接：{url}",
    "live_title": "【B站改标题】{name}\n旧标题：{old_title}\n新标题：{new_title}\n时间：{event_time}\n链接：{url}",
    "dynamic": "【B站动态】{name}\n{type_text}\n{content}\n时间：{event_time}\n链接：{url}",
    "collection": "【B站合集更新】{name}\n视频：{video_title}\n合集：{list_name}\n发布时间：{publish_time}\n链接：{url}",
    "alert": "【B站监控告警】{content}",
}

_logger: Any = None


def _get_logger() -> Any:
    """Return the AstrBot plugin logger, falling back to stdlib logging offline."""
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger(__name__)
        else:
            _logger = astrbot_logger
    return _logger


def _coerce(value: Any) -> str:
    """Convert a payload value to text, ``None`` becoming ``""``."""
    return "" if value is None else str(value)


def _template_fields(template: str) -> list[str]:
    """Extract ``format`` placeholder names from a template string."""
    return [name for _, name, _, _ in Formatter().parse(template) if name]


def _truncate(text: str, max_len: int = _MAX_TEXT_LEN) -> str:
    """Truncate ``text`` to ``max_len`` characters, appending an ellipsis."""
    if len(text) <= max_len:
        return text
    return text[: max_len - 1] + "…"


def _fallback_text(event_type: str, payload: dict) -> str:
    """Build the generic text used for unknown event types."""
    name = payload.get("name", event_type)
    parts = [f"【B站更新】{_coerce(name)}"]
    if payload.get("url"):
        parts.append(f"链接：{payload['url']}")
    return "\n".join(parts)


def text_for(event_type: str, payload: dict) -> str:
    """Render the push text for an event type and its payload.

    Args:
        event_type: Event type, one of ``live_on`` / ``live_off`` /
            ``live_title`` / ``dynamic`` / ``collection``, or anything else
            for the generic fallback text.
        payload: Event payload. Required keys per event type (placeholders
            match template placeholders, missing keys render as ``""``):
            - ``live_on``: name, title, area_name, live_start_time, url.
            - ``live_off``: name, duration, url.
            - ``live_title``: name, old_title, new_title, url.
            - ``dynamic``: name, type_text, content, url.
            - ``collection``: name, video_title, list_name, publish_time, url.
            ``cover`` (URL, optional) is only consumed by :func:`build_chain`.

    Returns:
        The rendered push text, truncated to :data:`_MAX_TEXT_LEN` characters.
    """
    template = _TEMPLATES.get(event_type)
    if template is None:
        return _truncate(_fallback_text(event_type, payload))
    values = {
        field: _coerce(payload.get(field)) for field in _template_fields(template)
    }
    return _truncate(template.format(**values))


def build_chain(event_type: str, payload: dict) -> Any:
    """Build the push content for an event type and its payload.

    Returns a ``MessageChain`` when the AstrBot API is importable, otherwise a
    plain ``str`` with the same text (offline tests / T17 smoke rely on this).
    A ``MessageChain`` carries the text plus an optional ``cover`` image;
    cover-image construction is wrapped in ``try/except`` so any failure
    degrades to text-only and logs a warning instead of aborting the push.

    Args:
        event_type: Event type, forwarded to :func:`text_for`.
        payload: Event payload, as documented in :func:`text_for`. ``cover``
            is an optional image URL appended when the AstrBot API exists.

    Returns:
        ``MessageChain`` when available, else ``str``. Callers pass this
        straight to ``context.send_message(session, chain)``.
    """
    text = text_for(event_type, payload)
    if not _ASTRBOT_AVAILABLE or MessageChain is None or Comp is None:
        return text
    chain = MessageChain().message(text)
    cover = payload.get("cover")
    if cover:
        try:
            # 封面固定追加在消息链**尾部**（文字在前）：部分平台（如飞书）对
            # 「文字+图片」混合消息链存在顺序兼容问题，尾部顺序可最大限度
            # 保证文字与图片同时送达；仍不兼容时可经 poll.push_*_cover 关闭。
            image = Comp.Image.fromURL(str(cover))
            chain.chain.append(image)
        except Exception as exc:  # noqa: BLE001 - 封面失败必须降级而非中断推送
            _get_logger().warning(
                "封面图片加载失败（event=%s），降级为纯文本推送: %s", event_type, exc
            )
    return chain


def _ensure_status(status: dict[str, Any], sub_id: str) -> Any:
    """Return the mutable status entry for ``sub_id``, creating a minimal one.

    The status dict is owned by the caller (main.py scheduler, T10) and maps
    ``sub_id`` to a mutable object exposing ``last_push_at`` / ``last_error``
    attributes. When the entry does not exist yet, a :class:`SimpleNamespace`
    with both attributes set to ``None`` is inserted.
    """
    entry = status.get(sub_id)
    if entry is None:
        entry = SimpleNamespace(last_push_at=None, last_error=None)
        status[sub_id] = entry
    return entry


def _now_iso() -> str:
    """Current UTC time as an ISO-8601 string (lexicographically sortable)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def format_event_time(timestamp: Any) -> str:
    """把 epoch 时间戳格式化为可读的 UTC+8（中国标准时间）；非法/非正值返回空串。

    Args:
        timestamp: epoch 秒（int/float/数字字符串均可）。

    Returns:
        ``YYYY-MM-DD HH:MM:SS``（UTC+8）字符串；无法解析时返回空串。
    """
    try:
        ts = int(timestamp)
    except (TypeError, ValueError):
        return ""
    if ts <= 0:
        return ""
    try:
        return datetime.fromtimestamp(ts, tz=_EVENT_TZ).strftime("%Y-%m-%d %H:%M:%S")
    except (OSError, OverflowError, ValueError):
        return ""


async def send(
    subscription: Subscription,
    chain: Any,
    context: Any,
    status: dict[str, Any],
) -> dict[str, bool]:
    """Deliver ``chain`` to every push session of ``subscription``.

    Each session is validated with :func:`validate_session` first; invalid
    sessions are skipped, logged, and recorded as ``False`` without raising.
    Every per-session outcome is logged (info on success, warning on failure),
    and failures (send exception, platform not found) never propagate out of
    this function — they are logged and reflected in the result map.

    Status bookkeeping (``status`` maps ``sub_id`` to a mutable object):
    - Any session success sets ``last_push_at`` to the current UTC ISO-8601
      time and clears ``last_error``.
    - All sessions failing records a ``last_error`` string.
    - A missing entry is created on demand (see :func:`_ensure_status`).

    Args:
        subscription: The subscription whose ``push_session_ids`` to target.
        chain: Anything returned by :func:`build_chain` (``MessageChain`` or
            ``str``) — passed verbatim to ``context.send_message``.
        context: The AstrBot ``Context`` (or a fake exposing
            ``async send_message(session, chain) -> bool``).
        status: Runtime status dict keyed by ``sub_id``, holding mutable
            status objects.

    Returns:
        Mapping of ``session -> success``. Invalid sessions are ``False``.
    """
    results: dict[str, bool] = {}
    any_success = False
    last_error: str | None = None
    for session in subscription.push_session_ids:
        try:
            validate_session(session)
        except ValueError as exc:
            _get_logger().warning("跳过非法推送会话 %s: %s", session, exc)
            results[session] = False
            if last_error is None:
                last_error = f"非法会话 {session}: {exc}"
            continue
        send_error: Exception | None = None
        try:
            ok = bool(await context.send_message(session, chain))
        except Exception as exc:  # noqa: BLE001 - 单会话失败不中断其余会话
            ok = False
            send_error = exc
        results[session] = ok
        if ok:
            _get_logger().info(
                "推送成功 → 会话 %s（订阅：%s）", session, subscription.name
            )
            any_success = True
        else:
            reason = (
                f"异常: {send_error}"
                if send_error is not None
                else "平台未找到或目标不可达"
            )
            _get_logger().warning(
                "推送失败 → 会话 %s（订阅：%s）: %s",
                session,
                subscription.name,
                reason,
            )
            if last_error is None:
                last_error = f"会话 {session} 发送失败: {reason}"
    if any_success:
        entry = _ensure_status(status, subscription.id)
        entry.last_push_at = _now_iso()
        entry.last_error = None
    elif last_error is not None:
        entry = _ensure_status(status, subscription.id)
        entry.last_error = last_error
    return results
