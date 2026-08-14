"""推送模板 / 时间格式化单元测试。

离线测试（无 AstrBot 运行时）验证三类推送模板均携带详细事件时间，以及
``format_event_time`` 对 epoch 时间戳的格式化与非法值兜底。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from push import format_event_time, text_for

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
            "type_text": "文字",
            "content": "内容",
            "event_time": "2023-11-14 22:13:20",
            "url": "https://t.bilibili.com/1",
        },
    )
    assert "【B站动态】" in text
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
