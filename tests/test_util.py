"""util.py 公共工具层单元测试。

重构后所有模块共享这一份小工具，这里把离线行为固化为回归闸：
logger 离线回退、ISO 时间可解析、noop acquire 可等待、类型标签回退。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from util import get_logger, noop_acquire, now_iso, type_label


def test_now_iso_returns_parseable_utc_timestamp() -> None:
    """now_iso 输出为 UTC ISO-8601 秒级时间戳。"""
    value = now_iso()
    parsed = datetime.fromisoformat(value)
    assert parsed.tzinfo is not None
    assert parsed.utcoffset().total_seconds() == 0
    assert parsed.microsecond == 0


def test_noop_acquire_is_awaitable_and_returns_none() -> None:
    """轮询器缺省取牌函数可直接 await，行为为空操作。"""

    async def scenario() -> None:
        assert await noop_acquire() is None

    asyncio.run(scenario())


def test_type_label_known_and_unknown() -> None:
    """已知订阅类型返回中文标签，未知类型原样返回。"""
    assert type_label("live") == "直播"
    assert type_label("dynamic") == "动态"
    assert type_label("collection") == "合集"
    assert type_label("unknown") == "unknown"


def test_get_logger_offline_falls_back_to_plugin_logger_name() -> None:
    """离线环境无 AstrBot 时回退 stdlib logger（缺省名为插件级 logger）。"""
    logger = get_logger()
    assert isinstance(logger, logging.Logger)
    assert logger.name == "astrbot_plugin_bilibili_cj"
