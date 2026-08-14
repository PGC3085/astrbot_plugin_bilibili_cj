"""配置往返与会话校验单元测试（计划 todo 4）。

覆盖三类行为：

1. **防裁剪回归**：``_conf_schema.json`` 将 ``subscriptions`` 声明为 ``list``，
   使 AstrBotConfig 的 ``check_config_integrity`` 走非 dict 分支原样透传，
   逐项字段（``id``/``list_id``/``series_type``/``push_session_ids`` 等）不被裁剪。
   测试：(a) schema 声明断言（回归闸）；(b) 对 ``check_config_integrity`` 的忠实
   复刻（astrbot_config.py:174-231）证明 list 项 verbatim 存活 + 对照用例证明
   object 声明会被裁剪；(c) ``normalize`` → ``to_dict`` → ``normalize`` 往返。
2. **validate_session**：``split(":", 2)`` 语义（与 ``MessageSession.from_str``
   一致），缺冒号拒绝、多余冒号归属 session_id。
3. **normalize**：合法三类型全接受；缺 ``list_id``/非法 type/空会话/uid 非法
   全拒绝；轮询钳制；稳定 id；缺省轮询间隔。

真实 ``AstrBotConfig`` 加载/保存往返仅在 AstrBot 运行时下执行（离线环境
``from astrbot.api import AstrBotConfig`` 无法导入，``skipif`` 跳过）。
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from config import (
    Subscription,
    _is_valid_session,
    _normalize_sessions,
    normalize,
    validate_session,
)

_ROOT = Path(__file__).resolve().parents[1]
_SCHEMA_PATH = _ROOT / "_conf_schema.json"


def _load_schema() -> dict:
    with open(_SCHEMA_PATH, encoding="utf-8") as f:
        return json.load(f)


def _full_config() -> dict:
    """构造三订阅（live/dynamic/collection）完整配置，每次调用返回新字典。"""
    return {
        "credential": {
            "sessdata": "",
            "bili_jct": "",
            "buvid3": "",
            "buvid4": "",
            "dedeuserid": "",
            "ac_time_value": "",
        },
        "poll": {
            "global_min_interval_sec": 60,
            "poll_jitter_sec": 15,
            "push_title_change": True,
        },
        "webui": {"enabled": True, "host": "127.0.0.1", "port": 8765, "token": ""},
        "subscriptions": [
            {
                "id": "live-1",
                "type": "live",
                "name": "示例直播",
                "uid": 835644,
                "poll_interval_sec": 120,
                "enabled": True,
                "push_session_ids": [
                    "aiocqhttp:GroupMessage:123",
                    "telegram:PrivateMessage:987",
                ],
            },
            {
                "id": "dyn-1",
                "type": "dynamic",
                "name": "示例动态",
                "uid": 12345,
                "poll_interval_sec": 60,
                "enabled": True,
                "push_session_ids": ["aiocqhttp:GroupMessage:456"],
            },
            {
                "id": "col-1",
                "type": "collection",
                "name": "示例合集",
                "uid": 999,
                "list_id": 123456,
                "series_type": 0,
                "poll_interval_sec": 300,
                "enabled": True,
                "push_session_ids": ["aiocqhttp:GroupMessage:789"],
            },
        ],
    }


# --------------------------------------------------------------------------
# AstrBotConfig 运行时探测（与 T2 spike 结论一致：离线环境不可导入）
# --------------------------------------------------------------------------
try:
    from astrbot.api import AstrBotConfig as _AstrBotConfig

    _HAS_ASTRBOT_RUNTIME = True
except ImportError:  # pragma: no cover - 离线测试环境
    _AstrBotConfig = None
    _HAS_ASTRBOT_RUNTIME = False

skip_without_astrbot = pytest.mark.skipif(
    not _HAS_ASTRBOT_RUNTIME,
    reason="AstrBot 运行时不可用：from astrbot.api import AstrBotConfig 无法导入",
)


# --------------------------------------------------------------------------
# AstrBotConfig.check_config_integrity 的忠实复刻（astrbot_config.py:174-231）
# --------------------------------------------------------------------------
_DEFAULT_VALUE_MAP = {
    "string": "",
    "int": 0,
    "float": 0.0,
    "bool": False,
    "list": [],
    "object": {},
    "text": "",
    "template_list": [],
}


def _replicate_schema_to_default(schema: dict) -> dict:
    """复刻 ``AstrBotConfig._config_schema_to_default_config``（147-172 行）。"""
    conf: dict = {}
    for key, spec in schema.items():
        if spec["type"] == "object":
            conf[key] = _replicate_schema_to_default(spec["items"])
        else:
            conf[key] = spec.get("default", _DEFAULT_VALUE_MAP.get(spec["type"]))
    return conf


def _replicate_check_config_integrity(refer_conf: dict, conf: dict) -> None:
    """复刻 ``check_config_integrity`` 的裁剪/透传语义。

    - dict 值（schema 声明 object）递归：子键不在 refer 中的会被裁剪；
    - 非 dict 值（含 ``list``）走 ``else`` 分支原样透传，逐项字段永不裁剪。
    """
    new_conf: dict = {}
    for key, value in refer_conf.items():
        if key not in conf or conf[key] is None:
            new_conf[key] = value
        elif isinstance(value, dict):
            if not isinstance(conf[key], dict):
                new_conf[key] = value
            else:
                _replicate_check_config_integrity(value, conf[key])
                new_conf[key] = conf[key]
        else:
            new_conf[key] = conf[key]
    conf.clear()
    conf.update(new_conf)


# --------------------------------------------------------------------------
# 1. 配置往返 / 防裁剪回归
# --------------------------------------------------------------------------
def test_schema_declares_subscriptions_as_list() -> None:
    """回归闸：``subscriptions.type`` 必须是 ``list``。

    若被改成 ``object``，AstrBotConfig 会递归裁剪未声明的逐项字段
    （``id``/``list_id``/``series_type``/``push_session_ids``），本测试必须红。
    """
    schema = _load_schema()
    assert schema["subscriptions"]["type"] == "list"
    assert schema["subscriptions"]["default"] == []


def test_replicated_integrity_keeps_list_items_verbatim() -> None:
    """复刻 ``check_config_integrity`` 下，list 订阅项逐字段原样存活。"""
    schema = _load_schema()
    refer = _replicate_schema_to_default(schema)
    conf = _full_config()
    original_items = copy.deepcopy(conf["subscriptions"])

    _replicate_check_config_integrity(refer, conf)

    assert conf["subscriptions"] == original_items
    for item, orig in zip(conf["subscriptions"], original_items):
        assert item == orig  # id/list_id/series_type/push_session_ids 等全保留


def test_replicated_integrity_strips_undeclared_object_fields() -> None:
    """对照：若 subscriptions 声明为 object，未声明字段会被裁剪——证明复刻真实。

    这解释了为什么 ``list`` 声明是逐项字段免裁剪的唯一防线。
    """
    refer = {"subscriptions": {"type": "", "uid": 0}}
    conf = {"subscriptions": {"type": "live", "id": "x", "uid": 1, "name": "n"}}

    _replicate_check_config_integrity(refer, conf)

    assert conf["subscriptions"] == {"type": "live", "uid": 1}


def test_normalize_roundtrip_preserves_all_fields() -> None:
    """normalize → to_dict → normalize 往返，关键字段全存活。"""
    original = _full_config()
    subs1 = normalize(copy.deepcopy(original))
    assert len(subs1) == 3

    subs2 = normalize({"subscriptions": [s.to_dict() for s in subs1]})
    assert len(subs2) == 3

    for s1, s2 in zip(subs1, subs2):
        assert s2.id == s1.id
        assert s2.type == s1.type
        assert s2.name == s1.name
        assert s2.uid == s1.uid
        assert s2.list_id == s1.list_id
        assert s2.series_type == s1.series_type
        assert s2.poll_interval_sec == s1.poll_interval_sec
        assert s2.enabled == s1.enabled
        assert s2.push_session_ids == s1.push_session_ids

    col = next(s for s in subs2 if s.type == "collection")
    assert col.list_id == 123456
    assert col.series_type == 0
    assert col.push_session_ids == ["aiocqhttp:GroupMessage:789"]
    live = next(s for s in subs2 if s.type == "live")
    assert live.uid == 835644
    assert live.poll_interval_sec == 120
    assert live.push_session_ids == [
        "aiocqhttp:GroupMessage:123",
        "telegram:PrivateMessage:987",
    ]


@skip_without_astrbot
def test_real_astrbotconfig_roundtrip_preserves_list(tmp_path: Path) -> None:
    """真实 AstrBotConfig 加载/保存往返（仅 AstrBot 运行时下执行）。

    离线环境跳过；运行时验证 ``subscriptions`` 项在 check_config_integrity
    处理后逐字段保留。
    """
    cfg_path = tmp_path / "plugin_config.json"
    cfg_path.write_text(
        json.dumps(_full_config(), ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    expected = _full_config()["subscriptions"]

    cfg = _AstrBotConfig(str(cfg_path), schema=_load_schema())

    assert cfg["subscriptions"] == expected


# --------------------------------------------------------------------------
# 2. validate_session / _is_valid_session（split(":", 2) 语义）
# --------------------------------------------------------------------------
def test_validate_session_accepts_valid() -> None:
    validate_session("aiocqhttp:GroupMessage:123")  # 不应抛异常


@pytest.mark.parametrize(
    "bad",
    [
        "aiocqhttp",  # 无冒号
        "aiocqhttp:GroupMessage",  # 单冒号 -> 仅 2 段
        "GroupMessage:123",  # 缺平台 -> 仅 2 段
        ":GroupMessage:123",  # 平台为空
        "aiocqhttp::123",  # 消息类型为空
    ],
)
def test_validate_session_rejects_missing_colons(bad: str) -> None:
    with pytest.raises(ValueError):
        validate_session(bad)


def test_validate_session_extra_colons_split_into_session_id() -> None:
    """多余冒号归属 session_id：a:b:c:d 切分为 a / b / c:d 且合法。"""
    session = "aiocqhttp:GroupMessage:123:456"
    validate_session(session)  # 接受

    parts = session.split(":", 2)
    assert parts[0] == "aiocqhttp"
    assert parts[1] == "GroupMessage"
    assert parts[2] == "123:456"
    assert _is_valid_session(session) is True


def test_is_valid_session_rejects_non_string() -> None:
    assert _is_valid_session(123) is False
    assert _is_valid_session(None) is False
    assert _is_valid_session(["aiocqhttp:GroupMessage:1"]) is False


def test_normalize_sessions_filters_invalid_keeps_valid() -> None:
    sessions = _normalize_sessions(
        ["aiocqhttp:GroupMessage:1", "bad", "telegram:PrivateMessage:2"]
    )
    assert sessions == [
        "aiocqhttp:GroupMessage:1",
        "telegram:PrivateMessage:2",
    ]
    assert _normalize_sessions(None) == []
    assert _normalize_sessions("not-a-list") == []
    assert _normalize_sessions([123, "a:b"]) == []  # 全非法


# --------------------------------------------------------------------------
# 3. normalize 接受合法三类型
# --------------------------------------------------------------------------
def test_normalize_accepts_three_types() -> None:
    subs = normalize(_full_config())
    assert len(subs) == 3
    assert all(isinstance(s, Subscription) for s in subs)

    by_type = {s.type: s for s in subs}
    assert set(by_type) == {"live", "dynamic", "collection"}

    live = by_type["live"]
    assert live.uid == 835644
    assert live.poll_interval_sec == 120
    assert live.push_session_ids == [
        "aiocqhttp:GroupMessage:123",
        "telegram:PrivateMessage:987",
    ]

    dyn = by_type["dynamic"]
    assert dyn.uid == 12345
    assert dyn.list_id is None

    col = by_type["collection"]
    assert col.uid == 999
    assert col.list_id == 123456
    assert col.series_type == 0


def test_normalize_tolerates_missing_poll() -> None:
    cfg = _full_config()
    del cfg["poll"]
    assert len(normalize(cfg)) == 3


# --------------------------------------------------------------------------
# 4. normalize 拒绝非法项
# --------------------------------------------------------------------------
def _single_sub_config(item: dict) -> dict:
    return {"subscriptions": [item]}


def test_normalize_rejects_collection_without_list_id() -> None:
    cfg = _single_sub_config(
        {
            "type": "collection",
            "uid": 999,
            "series_type": 0,
            "push_session_ids": ["aiocqhttp:GroupMessage:789"],
        }
    )
    assert normalize(cfg) == []


def test_normalize_rejects_invalid_type() -> None:
    cfg = _single_sub_config(
        {"type": "banana", "uid": 1, "push_session_ids": ["aiocqhttp:GroupMessage:1"]}
    )
    assert normalize(cfg) == []


def test_normalize_rejects_empty_push_sessions() -> None:
    cfg = _single_sub_config({"type": "live", "uid": 1, "push_session_ids": []})
    assert normalize(cfg) == []


def test_normalize_rejects_all_invalid_push_sessions() -> None:
    cfg = _single_sub_config(
        {"type": "live", "uid": 1, "push_session_ids": ["not-valid", "alsonot:valid"]}
    )
    assert normalize(cfg) == []  # 形状过滤后再判空


def test_normalize_rejects_non_numeric_uid() -> None:
    cfg = _single_sub_config(
        {"type": "live", "uid": "abc", "push_session_ids": ["aiocqhttp:GroupMessage:1"]}
    )
    assert normalize(cfg) == []


def test_normalize_skips_bad_items_keeps_good_ones() -> None:
    cfg = _full_config()
    cfg["subscriptions"].append(  # 非法项混入合法列表
        {"type": "live", "uid": "not-a-number", "push_session_ids": ["x"]}
    )
    subs = normalize(cfg)
    assert len(subs) == 3  # 非法项被跳过，其余保留


# --------------------------------------------------------------------------
# 5. 轮询钳制
# --------------------------------------------------------------------------
def test_poll_clamps_zero_min_interval_to_one() -> None:
    cfg = _full_config()
    cfg["poll"]["global_min_interval_sec"] = 0
    normalize(cfg)
    assert cfg["poll"]["global_min_interval_sec"] == 1


def test_poll_clamps_negative_jitter_to_zero() -> None:
    cfg = _full_config()
    cfg["poll"]["poll_jitter_sec"] = -5
    normalize(cfg)
    assert cfg["poll"]["poll_jitter_sec"] == 0.0


def test_poll_keeps_in_range_values_unchanged() -> None:
    cfg = _full_config()
    normalize(cfg)
    assert cfg["poll"]["global_min_interval_sec"] == 60
    assert cfg["poll"]["poll_jitter_sec"] == 15


# --------------------------------------------------------------------------
# 6. 稳定 id
# --------------------------------------------------------------------------
def test_existing_ids_preserved() -> None:
    subs = normalize(_full_config())
    assert [s.id for s in subs] == ["live-1", "dyn-1", "col-1"]


def test_missing_ids_generated_and_stable_across_roundtrip() -> None:
    cfg = _full_config()
    for item in cfg["subscriptions"]:
        item.pop("id", None)

    subs1 = normalize(cfg)
    assert all(s.id for s in subs1)  # 缺失 -> 生成 uuid4

    subs2 = normalize({"subscriptions": [s.to_dict() for s in subs1]})
    assert [s.id for s in subs2] == [s.id for s in subs1]  # 往返稳定


def test_generated_id_stable_across_to_dict_normalize() -> None:
    cfg = _full_config()
    cfg["subscriptions"] = [cfg["subscriptions"][0]]
    cfg["subscriptions"][0].pop("id", None)

    sub1 = normalize(cfg)[0]
    sub2 = normalize({"subscriptions": [sub1.to_dict()]})[0]

    assert sub2.id == sub1.id


# --------------------------------------------------------------------------
# 7. 缺省轮询间隔
# --------------------------------------------------------------------------
def test_default_poll_interval_when_missing() -> None:
    cfg = _full_config()
    for item in cfg["subscriptions"]:
        item.pop("poll_interval_sec", None)
    subs = normalize(cfg)
    assert [s.poll_interval_sec for s in subs] == [300, 300, 300]


# --------------------------------------------------------------------------
# 8. 字符串包裹形态还原（v1.0.2：AstrBot 配置表单把 list 存成字符串）
# --------------------------------------------------------------------------
def test_coerce_subscriptions_as_raw_json_string() -> None:
    """表单把整个数组存成 subscriptions 字符串值时解析还原。"""
    cfg = _full_config()
    cfg["subscriptions"] = json.dumps(cfg["subscriptions"], ensure_ascii=False)
    subs = normalize(cfg)
    assert len(subs) == 3
    assert all(isinstance(s, Subscription) for s in subs)


def test_coerce_subscriptions_single_wrapped_string() -> None:
    """表单把数组文本存成数组唯一字符串元素（["[...]"]）时还原。"""
    cfg = _full_config()
    cfg["subscriptions"] = [json.dumps(cfg["subscriptions"], ensure_ascii=False)]
    subs = normalize(cfg)
    assert len(subs) == 3
    assert all(isinstance(s, Subscription) for s in subs)


def test_coerce_subscription_items_as_json_strings() -> None:
    """表单把每个订阅对象都存成 JSON 字符串时逐项还原。"""
    cfg = _full_config()
    cfg["subscriptions"] = [
        json.dumps(item, ensure_ascii=False) for item in cfg["subscriptions"]
    ]
    subs = normalize(cfg)
    assert len(subs) == 3
    assert all(isinstance(s, Subscription) for s in subs)


def test_coerce_single_object_wrapped_string() -> None:
    """数组唯一元素是单个订阅对象的 JSON 字符串时还原为单元素列表。"""
    cfg = {
        "subscriptions": [
            json.dumps(
                {
                    "type": "live",
                    "uid": 1,
                    "push_session_ids": ["aiocqhttp:GroupMessage:1"],
                },
                ensure_ascii=False,
            )
        ]
    }
    subs = normalize(cfg)
    assert len(subs) == 1
    assert subs[0].uid == 1


def test_coerce_invalid_wrapped_string_skips_without_crash() -> None:
    """包裹形态的非法 JSON 不崩溃：原样进入逐项校验并跳过。"""
    assert normalize({"subscriptions": ["not-json-at-all"]}) == []
