"""平台指令查询 / 启动订阅日志 / 安装配置初始化 单元测试。

离线测试（无 AstrBot 运行时）验证：

1. ``query_session_subscriptions``：按会话过滤订阅并渲染为清单文本。
2. ``Scheduler.get_subscriptions``：返回当前订阅快照的副本。
3. ``PluginLifecycle._log_subscriptions`` / ``current_subscriptions``：
   启动时把订阅清单写入 logger，且 fake scheduler 无访问器时不崩溃。
4. ``ensure_config_file``：配置缺失时按 ``_conf_schema.json`` 默认值初始化，
   已存在时幂等不改写。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from config import Subscription
from main import (
    PluginLifecycle,
    ensure_config_file,
    query_session_subscriptions,
)
from scheduler import Scheduler

_SESSION = "aiocqhttp:GroupMessage:123"


def _make_logger(name: str = "test_commands") -> tuple[logging.Logger, Any]:
    """返回隔离的测试 logger + 记录 handler。"""
    handler = _RecordListHandler()
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger, handler


class _RecordListHandler(logging.Handler):
    """收集 LogRecord 的内存 handler。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class _SchedulerWithSubs:
    """仅暴露 ``get_subscriptions`` 的 fake scheduler。"""

    def __init__(self, subs: list[Subscription]) -> None:
        self._subscriptions = list(subs)

    def get_subscriptions(self) -> list[Subscription]:
        return list(self._subscriptions)


def _live_sub(sub_id: str, uid: int, name: str | None = None) -> Subscription:
    return Subscription(
        id=sub_id,
        type="live",
        name=name or f"主播{uid}",
        uid=uid,
        poll_interval_sec=120,
        enabled=True,
        push_session_ids=[_SESSION],
    )


def _dynamic_sub(sub_id: str, uid: int, session: str = _SESSION) -> Subscription:
    return Subscription(
        id=sub_id,
        type="dynamic",
        name=f"动态{uid}",
        uid=uid,
        poll_interval_sec=180,
        enabled=True,
        push_session_ids=[session],
    )


def _collection_sub(sub_id: str, uid: int, list_id: int) -> Subscription:
    return Subscription(
        id=sub_id,
        type="collection",
        name=f"合集{uid}",
        uid=uid,
        list_id=list_id,
        series_type=0,
        poll_interval_sec=300,
        enabled=False,
        push_session_ids=[_SESSION],
    )


# --------------------------------------------------------------------------
# 1. query_session_subscriptions
# --------------------------------------------------------------------------


def test_query_session_subscriptions_lists_matching_only() -> None:
    """只列出推送目标包含当前会话的订阅，其余会话的订阅不出现。"""
    subs = [
        _live_sub("a", 1),
        _dynamic_sub("b", 2),
        _dynamic_sub("c", 3, session="telegram:GroupMessage:9"),
    ]
    text = query_session_subscriptions(subs, _SESSION)
    assert "当前会话" in text and _SESSION in text
    assert "[直播] 主播1" in text
    assert "[动态] 动态2" in text
    assert "动态3" not in text  # 其他会话的订阅被过滤


def test_query_session_subscriptions_no_match() -> None:
    """无订阅时返回友好提示。"""
    text = query_session_subscriptions([], _SESSION)
    assert "没有订阅" in text


def test_query_session_subscriptions_collection_format() -> None:
    """collection 订阅展示 list_id / series_type，禁用状态标注正确。"""
    text = query_session_subscriptions([_collection_sub("c", 5, 42)], _SESSION)
    assert "[合集] 合集5" in text
    assert "list_id=42" in text
    assert "series_type=0" in text
    assert "禁用" in text


# --------------------------------------------------------------------------
# 2. Scheduler.get_subscriptions
# --------------------------------------------------------------------------


def test_scheduler_get_subscriptions_returns_copy() -> None:
    """返回副本：外部清空不影响内部快照，且不触发 bilibili_api 导入。"""
    subs = [_live_sub("a", 1)]
    scheduler = Scheduler(subscriptions=subs, credential_cfg={}, repo=object())
    got = scheduler.get_subscriptions()
    assert [s.id for s in got] == ["a"]
    got.clear()
    assert [s.id for s in scheduler.get_subscriptions()] == ["a"]


# --------------------------------------------------------------------------
# 3. PluginLifecycle.current_subscriptions / _log_subscriptions
# --------------------------------------------------------------------------


def test_lifecycle_logs_subscriptions() -> None:
    """启动订阅清单写入 logger；scheduler 暴露 get_subscriptions 时列出详情。"""
    subs = [_live_sub("a", 1), _collection_sub("c", 5, 42)]
    logger, handler = _make_logger("test_commands.lifecycle")
    lifecycle = PluginLifecycle(
        config={},
        context=None,
        credential_cfg={},
        db=None,
        scheduler=_SchedulerWithSubs(subs),
        reloader=None,
        logger=logger,
        webui=None,
    )
    assert [s.id for s in lifecycle.current_subscriptions()] == ["a", "c"]
    lifecycle._log_subscriptions()
    messages = [r.getMessage() for r in handler.records]
    assert any("已加载 2 条 B站订阅" in m for m in messages)
    assert any("[直播] 主播1" in m and "uid=1" in m for m in messages)
    assert any("[合集] 合集5" in m and "list_id=42" in m for m in messages)


def test_lifecycle_current_subscriptions_falls_back_empty() -> None:
    """fake scheduler 无 get_subscriptions 时返回空列表，不崩溃。"""
    lifecycle = PluginLifecycle(
        config={},
        context=None,
        credential_cfg={},
        db=None,
        scheduler=object(),  # 无 get_subscriptions
        reloader=None,
        logger=logging.getLogger("test_commands.fallback"),
        webui=None,
    )
    assert lifecycle.current_subscriptions() == []


# --------------------------------------------------------------------------
# 4. ensure_config_file
# --------------------------------------------------------------------------


def test_ensure_config_file_initializes_from_schema(tmp_path: Path) -> None:
    """配置缺失时按 schema 默认值初始化（含 credential/poll/webui/subscriptions）。"""
    logger, _handler = _make_logger("test_commands.ensure")
    path = tmp_path / "astrbot_plugin_bilibili_cj_config.json"

    result = ensure_config_file(path, logger=logger)

    assert result == path
    assert path.is_file()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert data["credential"] == {
        "sessdata": "",
        "bili_jct": "",
        "buvid3": "",
        "buvid4": "",
        "dedeuserid": "",
        "ac_time_value": "",
    }
    assert data["poll"]["global_min_interval_sec"] == 60
    assert data["poll"]["poll_jitter_sec"] == 15
    assert data["poll"]["push_title_change"] is True
    assert data["webui"]["enabled"] is True
    assert data["webui"]["port"] == 8765
    assert data["subscriptions"] == []


def test_ensure_config_file_idempotent(tmp_path: Path) -> None:
    """配置文件已存在时不改写内容。"""
    logger, _handler = _make_logger("test_commands.idempotent")
    path = tmp_path / "astrbot_plugin_bilibili_cj_config.json"
    path.write_text('{"custom": true}', encoding="utf-8")

    ensure_config_file(path, logger=logger)

    assert path.read_text(encoding="utf-8") == '{"custom": true}'
