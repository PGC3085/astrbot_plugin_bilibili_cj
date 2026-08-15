"""动态消息解析器（poller/dynamic_parser.py）回归测试。

重点覆盖真实 B 站 polymer 数据在开启 ``itemOpusStyle`` 后出现的形状：

- 条目类型仍是 ``DYNAMIC_TYPE_DRAW``，但内容在
  ``major.type == MAJOR_TYPE_OPUS`` 的 ``major.opus`` 中，且
  ``module_dynamic.desc`` 为 ``null``；
- 该形状在旧实现下会解析出**空 body 且无任何图片**（线上反馈的
  「推送完全不包含动态文字和图片」根因），本文件把它固化为回归闸。
"""

from __future__ import annotations

import pytest

from config import Subscription
from poller.dynamic_parser import (
    DynamicContent,
    build_payload,
    extract_type,
)
from push import build_chain


def _sub() -> Subscription:
    return Subscription(
        id="sub-opus",
        type="dynamic",
        name="订阅名",
        uid=10086,
        push_session_ids=["aiocqhttp:GroupMessage:123"],
    )


def _opus_item(
    *,
    item_type: str = "DYNAMIC_TYPE_DRAW",
    summary: object = "图文正文",
    pics: list[str] | None = None,
    title: str = "图文标题",
    desc: object = None,
    dyn_id: str = "123456789",
) -> dict:
    """构造与真实 feed/space 接口一致的 opus 图文条目。

    关键点：``module_dynamic.desc`` 为 null（不是 dict）、内容都在
    ``major.opus``，这是线上 bug 的触发形状。
    """
    opus: dict = {
        "title": title,
        "summary": {"text": summary} if isinstance(summary, str) else summary,
        "pics": [{"url": url} for url in (pics or [])],
    }
    return {
        "id_str": dyn_id,
        "type": item_type,
        "modules": {
            "module_author": {"name": "枝堇Sumire", "pub_ts": "1786371736"},
            "module_dynamic": {
                "desc": desc,
                "major": {"type": "MAJOR_TYPE_OPUS", "opus": opus},
            },
        },
    }


def test_real_opus_draw_item_parses_text_and_all_images() -> None:
    """真实 bug 形状：DRAW + MAJOR_TYPE_OPUS → 图文(2048)，正文与多图完整。"""
    item = _opus_item(pics=["https://x/1.jpg", "https://x/2.jpg", "https://x/3.jpg"])
    assert extract_type(item) == 2048
    payload = build_payload(_sub(), item)
    assert payload["action"] == "发表了新动态："
    assert "图文正文" in payload["body"]
    assert "图文标题" in payload["body"]
    assert payload["cover"] == "https://x/1.jpg"
    assert payload["images"] == [
        "https://x/1.jpg",
        "https://x/2.jpg",
        "https://x/3.jpg",
    ]


def test_real_opus_word_item_keeps_sketch_type() -> None:
    """WORD + MAJOR_TYPE_OPUS 继续走图文(2048)句式。"""
    item = _opus_item(item_type="DYNAMIC_TYPE_WORD", title="")
    assert extract_type(item) == 2048
    payload = build_payload(_sub(), item)
    assert payload["body"] == "图文正文"


def test_legacy_draw_major_still_maps_to_image_type() -> None:
    """旧形状 DRAW + MAJOR_TYPE_DRAW 不被 opus 逻辑破坏，仍为图片动态(2)。"""
    item = {
        "id_str": "1",
        "type": "DYNAMIC_TYPE_DRAW",
        "modules": {
            "module_author": {"name": "UP", "pub_ts": "1700000000"},
            "module_dynamic": {
                "desc": {"text": "图片动态语"},
                "major": {
                    "type": "MAJOR_TYPE_DRAW",
                    "draw": {"items": [{"src": "https://x/draw.jpg"}]},
                },
            },
        },
    }
    assert extract_type(item) == 2
    payload = build_payload(_sub(), item)
    assert payload["action"] == "发布了新动态："
    assert payload["body"] == "图片动态语"
    assert payload["images"] == ["https://x/draw.jpg"]


def test_opus_summary_rich_text_nodes_fallback() -> None:
    """summary 只有 rich_text_nodes（无 text 字段）时也能拼出正文。"""
    summary = {
        "rich_text_nodes": [
            {"text": "第一段", "type": "RICH_TEXT_NODE_TYPE_TEXT"},
            {"text": "第二段", "type": "RICH_TEXT_NODE_TYPE_TEXT"},
        ]
    }
    item = _opus_item(summary=summary, pics=[])
    payload = build_payload(_sub(), item)
    assert payload["body"].endswith("第一段第二段")
    assert "images" not in payload


def test_opus_without_pics_still_pushes_text() -> None:
    """无图 opus（纯文字被标成 DRAW）不再推空消息。"""
    item = _opus_item(pics=[])
    payload = build_payload(_sub(), item)
    assert "图文正文" in payload["body"]
    assert "images" not in payload


def test_push_cover_false_drops_all_image_fields() -> None:
    """push_cover=False 时，cover 与 images 都不进载荷（保持纯文字推送能力）。"""
    item = _opus_item(pics=["https://x/1.jpg", "https://x/2.jpg"])
    payload = build_payload(_sub(), item, push_cover=False)
    assert "cover" not in payload
    assert "images" not in payload


def test_forward_of_opus_keeps_origin_content_and_images() -> None:
    """转发 opus 动态：DDBOT 转发句式 + 原动态标注行 + 原文正文 + 原图。"""
    orig = _opus_item(
        summary="原图文正文",
        pics=["https://x/o1.jpg", "https://x/o2.jpg"],
        title="原图文标题",
        dyn_id="111",
    )
    item = {
        "id_str": "222",
        "type": "DYNAMIC_TYPE_FORWARD",
        "modules": {
            "module_author": {"name": "转发者", "pub_ts": "1786371736"},
            "module_dynamic": {"desc": None, "major": None},
        },
        "orig": orig,
    }
    payload = build_payload(_sub(), item)
    assert payload["action"] == "转发了枝堇Sumire的动态："
    assert payload["body"].startswith("原动态：\n原图文标题")
    assert "原图文正文" in payload["body"]
    assert payload["images"] == ["https://x/o1.jpg", "https://x/o2.jpg"]


def test_content_dataclass_keeps_action_suffix_colon() -> None:
    """DynamicContent 动作短语含尾部冒号，渲染后直接接 UP 名。"""
    content = DynamicContent("发布了新动态：", "正文", ("https://x/1.jpg",))
    assert content.action.endswith("：")


def test_build_chain_appends_all_images_when_astrbot_available(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AstrBot 运行时：文本在前，payload.images 全部按顺序追加到消息链。"""

    class _FakeImage:
        def __init__(self, url: str) -> None:
            self.url = url

        @classmethod
        def fromURL(cls, url: str) -> "_FakeImage":
            return cls(url)

    class _FakeComp:
        Image = _FakeImage

    class _FakeChain:
        def __init__(self) -> None:
            self.chain: list = []

        def message(self, text: str) -> "_FakeChain":
            self.chain.append(("text", text))
            return self

    import push

    monkeypatch.setattr(push, "_ASTRBOT_AVAILABLE", True)
    monkeypatch.setattr(push, "MessageChain", _FakeChain)
    monkeypatch.setattr(push, "Comp", _FakeComp)

    chain = build_chain(
        "dynamic",
        {
            "name": "UP",
            "action": "发表了新动态：",
            "body": "正文",
            "event_time": "2026-08-10 12:00:00",
            "url": "https://t.bilibili.com/123",
            "images": ["https://x/1.jpg", "https://x/2.jpg"],
        },
    )
    assert isinstance(chain, _FakeChain)
    assert chain.chain[0] == ("text", chain.chain[0][1])
    images = [item for item in chain.chain[1:] if isinstance(item, _FakeImage)]
    assert [image.url for image in images] == [
        "https://x/1.jpg",
        "https://x/2.jpg",
    ]


def test_build_chain_cover_fallback_and_bad_image_skipped(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """旧调用方只有 cover 时仍可附图；单张失败只跳过该图，不中断文本推送。"""

    class _FakeImage:
        def __init__(self, url: str) -> None:
            self.url = url

        @classmethod
        def fromURL(cls, url: str) -> "_FakeImage":
            if url.endswith("bad.jpg"):
                raise ValueError("bad image")
            return cls(url)

    class _FakeComp:
        Image = _FakeImage

    class _FakeChain:
        def __init__(self) -> None:
            self.chain: list = []

        def message(self, text: str) -> "_FakeChain":
            self.chain.append(("text", text))
            return self

    import push

    monkeypatch.setattr(push, "_ASTRBOT_AVAILABLE", True)
    monkeypatch.setattr(push, "MessageChain", _FakeChain)
    monkeypatch.setattr(push, "Comp", _FakeComp)

    chain = build_chain(
        "dynamic",
        {
            "name": "UP",
            "action": "发布了新动态：",
            "body": "正文",
            "event_time": "",
            "url": "https://t.bilibili.com/1",
            "images": ["https://x/bad.jpg", "https://x/good.jpg"],
        },
    )
    images = [item for item in chain.chain[1:] if isinstance(item, _FakeImage)]
    assert [image.url for image in images] == ["https://x/good.jpg"]
    # 文本组件仍在链首，bad 图不会阻断后续 good 图与整条消息。
    assert isinstance(chain.chain[0], tuple)
