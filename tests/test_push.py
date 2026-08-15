"""推送模板 / 时间格式化单元测试。

离线测试（无 AstrBot 运行时）验证三类推送模板均携带详细事件时间，
``format_event_time`` 对 epoch 时间戳的格式化与非法值兜底，以及
``push.send`` 逐会话记录推送结果日志。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from config import Subscription
from push import format_event_time, send, text_for

#: 与 push._EVENT_TZ 保持一致的测试时区（UTC+8）。
_TZ = timezone(timedelta(hours=8))


def test_format_event_time_formats_epoch_to_cst_seconds() -> None:
    """epoch 秒 → ``YYYY-MM-DD HH:MM:SS``（UTC+8）。"""
    ts = 1_700_000_000
    expected = datetime.fromtimestamp(ts, tz=_TZ).strftime("%Y-%m-%d %H:%M:%S")
    assert format_event_time(ts) == expected
    assert format_event_time(str(ts)) == expected  # 数字字符串同样可解析


def test_format_event_time_invalid_returns_empty() -> None:
    """非法 / 非正值时间戳返回空串。"""
    assert format_event_time(None) == ""
    assert format_event_time("abc") == ""
    assert format_event_time(0) == ""
    assert format_event_time(-1) == ""


def test_live_on_template_renders_start_time() -> None:
    text = text_for(
        "live_on",
        {
            "name": "主播",
            "title": "标题",
            "area_name": "分区",
            "live_start_time": "2023-11-14 22:13:20",
            "url": "https://live.bilibili.com/1",
        },
    )
    assert "【B站开播】" in text
    assert "开播时间：2023-11-14 22:13:20" in text


def test_live_off_template_renders_event_time() -> None:
    text = text_for(
        "live_off",
        {
            "name": "主播",
            "duration": 3600,
            "event_time": "2023-11-14 22:13:20",
            "url": "https://live.bilibili.com/1",
        },
    )
    assert "【B站下播】" in text
    assert "下播时间：2023-11-14 22:13:20" in text


def test_dynamic_template_renders_event_time() -> None:
    text = text_for(
        "dynamic",
        {
            "name": "UP",
            "action": "发布了新动态：",
            "body": "内容",
            "event_time": "2023-11-14 22:13:20",
            "url": "https://t.bilibili.com/1",
        },
    )
    assert "【B站动态】" in text
    assert "UP发布了新动态：" in text
    assert "内容" in text
    assert "时间：2023-11-14 22:13:20" in text


def test_collection_template_renders_publish_time() -> None:
    text = text_for(
        "collection",
        {
            "name": "合集订阅",
            "video_title": "视频",
            "list_name": "列表",
            "publish_time": "2023-11-14 22:13:20",
            "url": "https://www.bilibili.com/video/BV1",
        },
    )
    assert "【B站合集更新】" in text
    assert "发布时间：2023-11-14 22:13:20" in text


def test_truncation_keeps_url_at_end() -> None:
    """超长正文截断后，链接仍完整保留在消息末尾（不再被截掉）。"""

    text = text_for(
        "dynamic",
        {
            "name": "UP",
            "action": "发布了新动态：",
            "body": "很" * 1000,
            "event_time": "2023-11-14 22:13:20",
            "url": "https://t.bilibili.com/1",
        },
    )
    assert text.endswith("链接：https://t.bilibili.com/1")
    assert text.count("链接：") == 1  # 链接行不重复
    body = text.rsplit("\n链接：", 1)[0]
    assert len(body) <= 500  # 正文截断到上限，链接不计入


def test_truncation_keeps_url_for_live_on() -> None:
    """开播推送（live_on）超长标题同样保留链接。"""

    text = text_for(
        "live_on",
        {
            "name": "主播",
            "title": "标" * 1000,
            "area_name": "分区",
            "live_start_time": "2023-11-14 22:13:20",
            "url": "https://live.bilibili.com/1",
        },
    )
    assert text.endswith("链接：https://live.bilibili.com/1")
    body = text.rsplit("\n链接：", 1)[0]
    assert len(body) <= 500


def test_short_text_unaffected_by_url_reappend() -> None:
    """短文本：链接行仅出现一次、内容不变形。"""

    text = text_for(
        "dynamic",
        {
            "name": "UP",
            "action": "发布了新动态：",
            "body": "短内容",
            "event_time": "2023-11-14 22:13:20",
            "url": "https://t.bilibili.com/1",
        },
    )
    assert text.endswith("链接：https://t.bilibili.com/1")
    assert text.count("链接：") == 1
    assert "短内容" in text


def test_send_logs_per_session_results() -> None:
    """``push.send`` 逐会话记录推送结果日志（成功 info / 失败 warning）。"""

    class _RecordHandler(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: list[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)

    class _Ctx:
        def __init__(self) -> None:
            self.sent: list[str] = []

        async def send_message(self, session: str, chain: Any) -> bool:
            del chain
            self.sent.append(session)
            return session.endswith(":ok")

    async def scenario() -> None:
        handler = _RecordHandler()
        logger = logging.getLogger("push")
        logger.handlers.clear()
        logger.setLevel(logging.INFO)
        logger.addHandler(handler)
        try:
            sub = Subscription(
                id="s1",
                type="live",
                name="测试订阅",
                uid=1,
                push_session_ids=[
                    "aiocqhttp:GroupMessage:ok",
                    "aiocqhttp:GroupMessage:fail",
                ],
            )
            ctx = _Ctx()
            results = await send(sub, "测试链", ctx, {})
            assert results == {
                "aiocqhttp:GroupMessage:ok": True,
                "aiocqhttp:GroupMessage:fail": False,
            }
            assert ctx.sent == [
                "aiocqhttp:GroupMessage:ok",
                "aiocqhttp:GroupMessage:fail",
            ]
            infos = [
                r.getMessage() for r in handler.records if r.levelno == logging.INFO
            ]
            warns = [
                r.getMessage() for r in handler.records if r.levelno == logging.WARNING
            ]
            assert any(
                "推送成功" in m and "aiocqhttp:GroupMessage:ok" in m for m in infos
            )
            assert any(
                "推送失败" in m and "aiocqhttp:GroupMessage:fail" in m for m in warns
            )
        finally:
            logger.removeHandler(handler)

    asyncio.run(scenario())
