"""配置校验与规范化模块。

将 ``_conf_schema.json`` 定义的原始配置字典（尤其是 ``subscriptions`` 列表）
解析、校验并规范化为类型化的 :class:`Subscription` 对象列表。

``subscriptions`` 在 schema 中声明为裸 ``list``（避免 AstrBotConfig 对 object
类型的递归裁剪），因此其元素结构必须由本模块的 :func:`normalize` 在加载时校验。
"""

from __future__ import annotations

import json
import math
from dataclasses import dataclass, field
from typing import Any
from uuid import uuid4

try:
    from astrbot.api import logger
except ImportError:  # pragma: no cover - 离线测试环境无 AstrBot 运行时
    import logging

    logger = logging.getLogger("astrbot_plugin_bilibili_cj")

SUBSCRIPTION_TYPES: tuple[str, ...] = ("live", "dynamic", "collection")
"""支持的订阅类型。"""

DEFAULT_POLL_INTERVAL_SEC: int = 300
"""订阅缺失 ``poll_interval_sec`` 时的默认轮询间隔（秒）。"""

POLL_MIN_INTERVAL_MIN: int = 1
"""``global_min_interval_sec`` 的下限，低于此值被钳制（避免 T10 速率 1/间隔 除零）。"""

POLL_JITTER_MIN: float = 0.0
"""``poll_jitter_sec`` 的下限，低于此值被钳制（避免负随机延迟）。"""

POLL_SUB_INTERVAL_MIN: int = 1
"""订阅 ``poll_interval_sec`` 的下限（秒）：0/负数/截断后为 0 均钳制到此值，
NaN/inf 等非有限数值视为非法并跳过该订阅。"""

_REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "live": ("uid",),
    "dynamic": ("uid",),
    "collection": ("uid", "list_id", "series_type"),
}
"""各订阅类型的必填字段。"""

_NO_PROACTIVE_PLATFORMS: frozenset[str] = frozenset(
    {"qq_official", "qq_official_webhook"}
)
"""已知不支持主动消息推送的适配器平台集合。

依据 ``AStrBot/astrbot/core/star/context.py:633``：qq_official（QQ 官方
API 平台）不支持 ``send_message`` 主动发送；qq_official_webhook 为其
Webhook 变体，同样依赖被动事件、无法可靠主动推送。能力检查只对**已知
不支持**的平台拦截，未知平台一律放行，避免误杀未来新增的适配器。
"""


@dataclass
class Subscription:
    """规范化后的订阅条目。

    Attributes:
        id: 订阅的唯一稳定标识（uuid4 字符串）。WebUI 往返编辑时保留原值，
            缺失时才新生成；后续状态库/去重/推送一律以此为主键。
        type: 订阅类型，取值 ``live`` / ``dynamic`` / ``collection``。
        name: 订阅名称，用于日志与 WebUI 显示。
        uid: B站用户 UID。
        list_id: 合集/列表 ID，仅 ``collection`` 使用。
        series_type: 合集类型（0=视频合集，1=收藏夹），仅 ``collection`` 使用。
        poll_interval_sec: 该订阅的轮询间隔（秒）。
        enabled: 是否启用。
        push_session_ids: 过滤非法格式后的推送目标会话 ID 列表
            （``platform:message_type:session_id`` 形状）。
    """

    id: str
    type: str
    name: str
    uid: int | None = None
    list_id: int | None = None
    series_type: int | None = None
    poll_interval_sec: int = DEFAULT_POLL_INTERVAL_SEC
    enabled: bool = True
    push_session_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        """转换为可 JSON 序列化的字典，用于写回配置文件。

        Returns:
            与原始订阅项同结构的字典。
        """
        return {
            "id": self.id,
            "type": self.type,
            "name": self.name,
            "uid": self.uid,
            "list_id": self.list_id,
            "series_type": self.series_type,
            "poll_interval_sec": self.poll_interval_sec,
            "enabled": self.enabled,
            "push_session_ids": list(self.push_session_ids),
        }


def _is_valid_session(session: str) -> bool:
    """判断会话字符串是否符合 ``platform:message_type:session_id`` 形状。

    与 AstrBot ``MessageSession.from_str``（``astrbot/core/platform/message_session.py``）
    的 ``split(":", 2)`` 切分语义保持一致：至少需要两个冒号，session_id 可含冒号
    （``a:b:c:d`` 切分为 ``a`` / ``b`` / ``c:d``）。此处不校验 platform 的平台能力，
    那是 todo 15 ``check_platform_capability`` 的职责。

    Args:
        session: 待校验的会话字符串。

    Returns:
        形状合法返回 ``True``，否则 ``False``。
    """
    if not isinstance(session, str):
        return False
    parts = session.split(":", 2)
    return len(parts) == 3 and bool(parts[0]) and bool(parts[1])


def validate_session(session: str) -> None:
    """校验会话字符串，非法时抛出 :class:`ValueError`。

    Args:
        session: 待校验的会话字符串。

    Raises:
        ValueError: 形状非法（缺少冒号、平台/消息类型为空等）。
    """
    if not _is_valid_session(session):
        raise ValueError(
            f"非法会话字符串 {session!r}，应为 'platform:message_type:session_id' "
            "（至少包含两个冒号）"
        )


def check_platform_capability(session: str) -> tuple[bool, str]:
    """检查会话所在平台是否支持主动消息推送。

    取 ``platform:message_type:session_id`` 的 platform 段，与
    :data:`_NO_PROACTIVE_PLATFORMS` 比对。未知平台 id 一律放行——能力检查
    只对**已知不支持**主动消息的平台返回拒绝，避免误杀未来新增的适配器。

    Args:
        session: 会话字符串（``platform:message_type:session_id`` 形状）。

    Returns:
        ``(True, "")``：平台支持主动消息（或未知平台，放行）。
        ``(False, reason)``：平台已知不支持主动消息，``reason`` 为告警文本。
    """
    platform = session.split(":", 1)[0]
    if platform in _NO_PROACTIVE_PLATFORMS:
        return False, f"平台不支持主动消息: {platform}"
    return True, ""


def _clamp_poll_settings(config: dict[str, Any]) -> None:
    """就地钳制轮询设置，越界值记告警并修正。

    ``global_min_interval_sec`` 钳制为 ≥ :data:`POLL_MIN_INTERVAL_MIN`，
    ``poll_jitter_sec`` 钳制为 ≥ :data:`POLL_JITTER_MIN`。否则 T10 的
    ``1 / global_min_interval_sec`` 速率会因除零/负速率崩溃。

    Args:
        config: 原始配置字典，``poll`` 项会被就地修改。
    """
    poll = config.get("poll")
    if not isinstance(poll, dict):
        return
    interval = poll.get("global_min_interval_sec")
    if isinstance(interval, (int, float)) and not isinstance(interval, bool):
        try:
            interval_value = float(interval)
        except (TypeError, ValueError, OverflowError):
            interval_value = math.nan
        if not math.isfinite(interval_value) or interval_value < POLL_MIN_INTERVAL_MIN:
            logger.warning(
                "global_min_interval_sec=%s 非法或低于下限 %s，已钳制为 %s",
                interval,
                POLL_MIN_INTERVAL_MIN,
                POLL_MIN_INTERVAL_MIN,
            )
            poll["global_min_interval_sec"] = POLL_MIN_INTERVAL_MIN
    jitter = poll.get("poll_jitter_sec")
    if isinstance(jitter, (int, float)) and not isinstance(jitter, bool):
        try:
            jitter_value = float(jitter)
        except (TypeError, ValueError, OverflowError):
            jitter_value = math.nan
        if not math.isfinite(jitter_value) or jitter_value < POLL_JITTER_MIN:
            logger.warning(
                "poll_jitter_sec=%s 非法或低于下限 %s，已钳制为 %s",
                jitter,
                POLL_JITTER_MIN,
                POLL_JITTER_MIN,
            )
            poll["poll_jitter_sec"] = POLL_JITTER_MIN


def _to_int_or_none(raw: Any) -> int | None:
    """把原始值转换为 int，非法返回 ``None``。

    Args:
        raw: 原始值（数字或数字字符串）。

    Returns:
        转换后的 int，无法转换时返回 ``None``。
    """
    if isinstance(raw, bool):  # bool 是 int 子类，单独排除
        return None
    if isinstance(raw, int):
        return raw
    if isinstance(raw, float) and raw.is_integer():
        return int(raw)
    if isinstance(raw, str) and raw.strip():
        try:
            return int(raw.strip())
        except ValueError:
            return None
    return None


def coerce_bool(value: Any, default: bool = True) -> bool:
    """把配置值解析为布尔。

    关键点：手改配置时常把布尔写成字符串（如 ``"false"``），而 Python 的
    ``bool("false")`` 为 ``True``——会把用户关闭的开关静默重新打开。这里
    显式识别常见真假字符串形态，其余类型回退默认值。

    Args:
        value: 原始配置值（bool / str / int / float 等）。
        default: 无法识别时的默认值。

    Returns:
        解析出的布尔值。
    """
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        stripped = value.strip().lower()
        if stripped in ("1", "true", "yes", "on"):
            return True
        if stripped in ("0", "false", "no", "off", ""):
            return False
    if isinstance(value, (int, float)):
        return bool(value)
    return default


def _normalize_sessions(raw_sessions: Any) -> list[str]:
    """过滤出形状合法的会话字符串。

    注意：**空数组判定必须在会话合法性/平台能力过滤之后执行**（见 todo 15）。
    此处仅做形状过滤——若订阅的全部会话都非法，则过滤后为空数组，由调用方据此
    判定订阅无效；todo 15 的 ``check_platform_capability`` 会在此处之后追加
    平台能力过滤，同样先过滤再判空。

    Args:
        raw_sessions: 原始 ``push_session_ids`` 值。

    Returns:
        过滤后的合法会话列表。
    """
    if not isinstance(raw_sessions, list):
        return []
    valid: list[str] = []
    for session in raw_sessions:
        if isinstance(session, str) and _is_valid_session(session):
            valid.append(session)
        else:
            logger.warning("忽略非法会话字符串 %r", session)
    return valid


def _normalize_subscription(raw: Any, index: int) -> Subscription | None:
    """校验并规范化单个订阅项，非法时记告警并返回 ``None``。

    Args:
        raw: 原始订阅项（预期为 dict）。
        index: 订阅在列表中的下标，用于告警定位。

    Returns:
        规范化后的 :class:`Subscription`；非法项返回 ``None``。
    """
    if not isinstance(raw, dict):
        logger.warning("订阅项 #%d 不是对象，已跳过", index)
        return None

    sub_type = raw.get("type")
    if sub_type not in SUBSCRIPTION_TYPES:
        logger.warning(
            "订阅项 #%d 的 type=%r 非法（可选 %s），已跳过",
            index,
            sub_type,
            SUBSCRIPTION_TYPES,
        )
        return None

    missing = [f for f in _REQUIRED_FIELDS[sub_type] if raw.get(f) is None]
    if missing:
        logger.warning(
            "订阅项 #%d（type=%s）缺少必填字段 %s，已跳过", index, sub_type, missing
        )
        return None

    uid = _to_int_or_none(raw.get("uid"))
    if uid is None:
        logger.warning("订阅项 #%d（type=%s）uid 非法，已跳过", index, sub_type)
        return None

    list_id: int | None = None
    series_type: int | None = None
    if sub_type == "collection":
        list_id = _to_int_or_none(raw.get("list_id"))
        series_type = _to_int_or_none(raw.get("series_type"))
        if list_id is None or series_type is None:
            logger.warning(
                "订阅项 #%d（collection）的 list_id/series_type 非法，已跳过", index
            )
            return None

    sessions = _normalize_sessions(raw.get("push_session_ids"))
    # 平台能力过滤（todo 15）：剔除已知不支持主动消息的平台的会话（如
    # qq_official），该过滤先于下方空检查执行——若全部会话都在不支持主动消息
    # 的平台上，订阅无有效推送目标，视为无效项跳过（r21/r22 F5 顺序）。
    capable_sessions: list[str] = []
    for session in sessions:
        ok, _ = check_platform_capability(session)
        if ok:
            capable_sessions.append(session)
        else:
            logger.warning("会话 %s 平台不支持主动消息，已剔除", session)
    sessions = capable_sessions
    # 空检查在会话合法性过滤之后执行：若全部会话非法/缺失，订阅无有效推送目标，
    # 视为无效项跳过（todo 15 的平台能力过滤也发生在此处之前）。
    if not sessions:
        logger.warning(
            "订阅项 #%d（type=%s, uid=%s）没有有效 push_session_ids，已跳过",
            index,
            sub_type,
            uid,
        )
        return None

    poll_interval = raw.get("poll_interval_sec")
    if poll_interval is None:
        poll_interval = DEFAULT_POLL_INTERVAL_SEC
    elif not isinstance(poll_interval, (int, float)) or isinstance(poll_interval, bool):
        logger.warning(
            "订阅项 #%d（type=%s, uid=%s）的 poll_interval_sec 非法，已跳过",
            index,
            sub_type,
            uid,
        )
        return None
    try:
        interval_value = float(poll_interval)
    except (TypeError, ValueError, OverflowError):
        logger.warning(
            "订阅项 #%d（type=%s, uid=%s）的 poll_interval_sec 无法解析，已跳过",
            index,
            sub_type,
            uid,
        )
        return None
    if not math.isfinite(interval_value):
        # NaN / inf：钳制无意义且会毒化调度（sleep(inf) 永久挂起），跳过该订阅
        logger.warning(
            "订阅项 #%d（type=%s, uid=%s）的 poll_interval_sec 非有限数值，已跳过",
            index,
            sub_type,
            uid,
        )
        return None
    if interval_value < POLL_SUB_INTERVAL_MIN:
        logger.warning(
            "订阅项 #%d（type=%s, uid=%s）的 poll_interval_sec=%s 低于下限 %s，已钳制",
            index,
            sub_type,
            uid,
            poll_interval,
            POLL_SUB_INTERVAL_MIN,
        )
        interval_value = float(POLL_SUB_INTERVAL_MIN)
    poll_interval_sec = int(interval_value)

    enabled = coerce_bool(raw.get("enabled"), True)

    raw_id = raw.get("id")
    sub_id = raw_id if isinstance(raw_id, str) and raw_id else str(uuid4())

    raw_name = raw.get("name")
    name = (
        str(raw_name) if isinstance(raw_name, str) and raw_name else f"{sub_type}:{uid}"
    )

    return Subscription(
        id=sub_id,
        type=sub_type,
        name=name,
        uid=uid,
        list_id=list_id,
        series_type=series_type,
        poll_interval_sec=poll_interval_sec,
        enabled=enabled,
        push_session_ids=sessions,
    )


def _coerce_subscriptions(raw_subs: Any) -> Any:
    """还原 AstrBot 配置表单可能产生的字符串包裹形态。

    AstrBot 管理面板对 ``list`` 类型配置字段，用户在输入框粘贴的 JSON 文本
    可能被以字符串形态保存：整个数组文本作为 ``subscriptions`` 的值、作为
    数组的单个字符串元素（``["[...]"]``），或每个订阅对象为 JSON 字符串
    （``["{...}", "{...}"]``）。此处尽力还原为真正的列表/字典；解析失败则
    原样返回，交由逐项校验记告警跳过（绝不因形态异常而崩溃）。

    Args:
        raw_subs: ``config["subscriptions"]`` 的原始值。

    Returns:
        还原后的订阅列表；无法还原时返回原值。
    """

    def _parse(text: str) -> Any:
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    if isinstance(raw_subs, str):
        parsed = _parse(raw_subs)
        if isinstance(parsed, list):
            return parsed
        if isinstance(parsed, dict):
            return [parsed]
        return raw_subs
    if isinstance(raw_subs, list):
        if len(raw_subs) == 1 and isinstance(raw_subs[0], str):
            parsed = _parse(raw_subs[0])
            if isinstance(parsed, list):
                return parsed
            if isinstance(parsed, dict):
                return [parsed]
            return raw_subs
        if raw_subs and all(isinstance(item, str) for item in raw_subs):
            coerced: list[Any] = []
            for item in raw_subs:
                parsed = _parse(item)
                coerced.append(parsed if parsed is not None else item)
            return coerced
    return raw_subs


def normalize(config: dict[str, Any]) -> list[Subscription]:
    """校验并规范化配置，返回有效的订阅列表。

    职责：
    - 就地钳制 ``poll`` 下的轮询设置（越界记告警并修正）。
    - 逐项校验 ``subscriptions``：type 合法、必填字段齐全、会话形状合法且过滤后
      非空、``poll_interval_sec`` 为数字；非法项记告警并跳过。
    - 为每项分配稳定 ``id``：WebUI 往返保留原值，缺失才新生成 uuid4。

    Args:
        config: 原始配置字典（来自 ``_conf_schema.json`` 或磁盘 JSON）。

    Returns:
        规范化后的有效 :class:`Subscription` 列表。
    """
    _clamp_poll_settings(config)

    if "subscriptions" not in config:
        logger.warning("配置缺少 subscriptions 键，按空列表处理（疑似手改笔误）")
        return []
    raw_subs = _coerce_subscriptions(config.get("subscriptions"))
    if raw_subs is None:
        return []
    if not isinstance(raw_subs, list):
        logger.warning(
            "subscriptions 应为数组，实际为 %s，已按空处理", type(raw_subs).__name__
        )
        return []

    result: list[Subscription] = []
    for index, raw in enumerate(raw_subs):
        sub = _normalize_subscription(raw, index)
        if sub is not None:
            result.append(sub)
    return result
