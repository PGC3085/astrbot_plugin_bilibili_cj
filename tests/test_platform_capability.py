"""平台能力检查单元测试（计划 todo 15）。

覆盖四类行为：

1. **check_platform_capability 拒绝**：qq_official / qq_official_webhook
   为已知不支持主动消息的适配器（``AStrBot/core/star/context.py:633``），
   返回 ``(False, reason)``。
2. **check_platform_capability 放行**：aiocqhttp / telegram 等支持主动消息
   的适配器返回 ``(True, "")``；未知平台 id 也放行（能力检查只对已知
   不支持的平台拦截）。
3. **normalize 集成（r21/r22 F5 顺序）**：能力过滤在形状过滤之后、空检查
   之前——仅含不支持平台会话的订阅整体被拒；混合会话只剔除不支持平台的
   那条。
4. **既有行为不回归**：既有 T4 用例（全量套件）保持绿。
"""

from __future__ import annotations

import pytest

from config import _NO_PROACTIVE_PLATFORMS, check_platform_capability, normalize


def _sub(item: dict) -> dict:
    """构造单个订阅的配置字典。"""
    return {"subscriptions": [item]}


# --------------------------------------------------------------------------
# 1. 已知不支持主动消息的适配器 -> 拒绝
# --------------------------------------------------------------------------
@pytest.mark.parametrize("platform", sorted(_NO_PROACTIVE_PLATFORMS))
def test_check_platform_capability_rejects_known_unsupported(platform: str) -> None:
    """qq_official / qq_official_webhook 会话被拒，reason 含平台 id。"""
    session = f"{platform}:GroupMessage:123"
    ok, reason = check_platform_capability(session)
    assert ok is False
    assert reason == f"平台不支持主动消息: {platform}"


def test_check_platform_capability_reason_names_platform() -> None:
    """reason 精确文本（WebUI/日志可读）。"""
    _, reason = check_platform_capability("qq_official:GroupMessage:123")
    assert reason == "平台不支持主动消息: qq_official"


# --------------------------------------------------------------------------
# 2. 支持主动消息 / 未知平台 -> 放行
# --------------------------------------------------------------------------
@pytest.mark.parametrize(
    "session",
    [
        "aiocqhttp:GroupMessage:123",
        "telegram:PrivateMessage:987",
        "discord:GroupMessage:42",
        "kook:GroupMessage:7",
        "lark:GroupMessage:9",
    ],
)
def test_check_platform_capability_allows_supported_platforms(session: str) -> None:
    """aiocqhttp/telegram 等支持主动消息的适配器返回 (True, "")。"""
    ok, reason = check_platform_capability(session)
    assert ok is True
    assert reason == ""


def test_check_platform_capability_allows_unknown_platform() -> None:
    """未知平台 id 放行——能力检查只对已知不支持拦截，不误杀新适配器。"""
    ok, reason = check_platform_capability("myplatform:GroupMessage:1")
    assert ok is True
    assert reason == ""


def test_check_platform_capability_ignores_extra_colons_in_session_id() -> None:
    """多余冒号归属 session_id，platform 提取不受影响。"""
    ok, _ = check_platform_capability("aiocqhttp:GroupMessage:123:456")
    assert ok is True
    ok, _ = check_platform_capability("qq_official:GroupMessage:123:456")
    assert ok is False


# --------------------------------------------------------------------------
# 3. normalize 集成：能力过滤先于空检查（F5 顺序）
# --------------------------------------------------------------------------
def test_normalize_drops_sub_with_only_unsupported_platform_sessions() -> None:
    """仅含 qq_official 会话的订阅：能力过滤后为空 -> 订阅整体被拒。"""
    cfg = _sub(
        {
            "type": "live",
            "uid": 1,
            "push_session_ids": [
                "qq_official:GroupMessage:111",
                "qq_official_webhook:GroupMessage:222",
            ],
        }
    )
    assert normalize(cfg) == []


def test_normalize_keeps_mixed_sessions_filters_unsupported() -> None:
    """混合会话：保留 aiocqhttp，剔除 qq_official，订阅仍有效。"""
    cfg = _sub(
        {
            "type": "live",
            "uid": 1,
            "push_session_ids": [
                "aiocqhttp:GroupMessage:123",
                "qq_official:GroupMessage:111",
            ],
        }
    )
    subs = normalize(cfg)
    assert len(subs) == 1
    assert subs[0].push_session_ids == ["aiocqhttp:GroupMessage:123"]


def test_normalize_keeps_unknown_platform_sessions() -> None:
    """未知平台会话被保留（能力检查只拦已知不支持）。"""
    cfg = _sub(
        {
            "type": "live",
            "uid": 1,
            "push_session_ids": ["myplatform:GroupMessage:1"],
        }
    )
    subs = normalize(cfg)
    assert len(subs) == 1
    assert subs[0].push_session_ids == ["myplatform:GroupMessage:1"]
