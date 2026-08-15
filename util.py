"""跨模块共享的最小公共工具层。

只放**确实重复 3 次以上**、且与业务无关的小工具，避免每个模块各自复制：

- :func:`get_logger`：AstrBot 运行时返回插件统一 logger 代理，离线环境
  回退 stdlib logger（按调用模块名区分）。
- :func:`now_iso`：当前 UTC 时间 ISO-8601 字符串（字典序可排序）。
- :func:`noop_acquire`：轮询器的缺省取牌函数（未注入令牌桶时行为不变）。
- :func:`type_label`：订阅类型的中文标签（入口指令与启动日志共用）。

本模块不依赖 AstrBot、不依赖 B 站 SDK，任何模块都可安全导入。
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any

try:
    from astrbot.api import logger as _astrbot_logger
except ImportError:  # pragma: no cover - 离线测试环境无 AstrBot 运行时
    _astrbot_logger = None

#: 订阅类型中文标签（启动日志与平台指令展示用）。
SUBSCRIPTION_TYPE_LABELS: dict[str, str] = {
    "live": "直播",
    "dynamic": "动态",
    "collection": "合集",
}

#: 未显式指定模块名时的离线 logger 名（沿用历史上插件级 logger 名称）。
_DEFAULT_LOGGER_NAME: str = "astrbot_plugin_bilibili_cj"


def get_logger(name: str | None = None) -> Any:
    """返回插件统一 logger；离线环境按 ``name`` 回退 stdlib logger。

    Args:
        name: 调用模块名（通常传 ``__name__``）。AstrBot 运行时忽略该参数
            （框架 logger 是代理对象，会按实际调用点自动路由到插件 logger）。

    Returns:
        AstrBot 插件 logger 代理，或离线环境下的 stdlib logger。
    """
    if _astrbot_logger is not None:
        return _astrbot_logger
    return logging.getLogger(name or _DEFAULT_LOGGER_NAME)


def now_iso() -> str:
    """当前 UTC 时间的 ISO-8601 字符串（字典序可排序）。"""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


async def noop_acquire() -> None:
    """默认无操作取牌：未注入令牌桶时轮询行为与之前完全一致。"""
    return None


def type_label(sub_type: str) -> str:
    """返回订阅类型的中文标签，未知类型原样返回。"""
    return SUBSCRIPTION_TYPE_LABELS.get(sub_type, sub_type)
