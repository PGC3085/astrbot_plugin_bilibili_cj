"""插件目录批量配置读取 / 深度合并 单元测试。

离线测试（无 AstrBot 运行时）验证：

1. ``read_config_file``：合法 JSON 对象返回 dict；非法 JSON / 顶层非对象 /
   文件缺失均返回 None 并告警。
2. ``_deep_merge``：dict 递归合并（保留下层缺省键），list 整体覆盖。
3. ``_bundled_config_path``：插件目录未放置 config.json 时返回 None（保证
   批量导入为 opt-in，不误载）。
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any

from main import _bundled_config_path, _deep_merge, read_config_file


def _make_logger(name: str = "test_bundled_config") -> tuple[logging.Logger, Any]:
    handler = _RecordListHandler()
    logger = logging.getLogger(name)
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger, handler


class _RecordListHandler(logging.Handler):
    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


# --------------------------------------------------------------------------
# 1. read_config_file
# --------------------------------------------------------------------------


def test_read_config_file_valid(tmp_path: Path) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps({"subscriptions": [{"type": "live"}]}, ensure_ascii=False),
        encoding="utf-8",
    )
    data = read_config_file(path)
    assert data == {"subscriptions": [{"type": "live"}]}


def test_read_config_file_bad_json(tmp_path: Path) -> None:
    logger, handler = _make_logger("test_bundled_config.bad")
    path = tmp_path / "config.json"
    path.write_text("{not valid", encoding="utf-8")

    assert read_config_file(path, logger) is None
    assert any("读取配置文件" in r.getMessage() for r in handler.records)


def test_read_config_file_non_object(tmp_path: Path) -> None:
    logger, handler = _make_logger("test_bundled_config.nonobj")
    path = tmp_path / "config.json"
    path.write_text("[1, 2, 3]", encoding="utf-8")

    assert read_config_file(path, logger) is None
    assert any("顶层不是对象" in r.getMessage() for r in handler.records)


def test_read_config_file_missing(tmp_path: Path) -> None:
    assert read_config_file(tmp_path / "nope.json") is None


# --------------------------------------------------------------------------
# 2. _deep_merge
# --------------------------------------------------------------------------


def test_deep_merge_recurses_dicts_and_replaces_lists() -> None:
    base: dict[str, Any] = {
        "credential": {"sessdata": "", "bili_jct": "keep"},
        "poll": {"global_min_interval_sec": 60, "poll_jitter_sec": 15},
        "subscriptions": [{"type": "live"}],
        "webui": {"enabled": True, "port": 8765},
    }
    override: dict[str, Any] = {
        "credential": {"sessdata": "abc"},
        "poll": {"global_min_interval_sec": 120},
        "subscriptions": [{"type": "dynamic"}, {"type": "collection"}],
    }

    _deep_merge(base, override)

    # dict 递归合并：override 覆盖指定键，其余键保留
    assert base["credential"] == {"sessdata": "abc", "bili_jct": "keep"}
    assert base["poll"] == {"global_min_interval_sec": 120, "poll_jitter_sec": 15}
    # list 整体覆盖
    assert base["subscriptions"] == [{"type": "dynamic"}, {"type": "collection"}]
    # 未出现在 override 的键保持不变
    assert base["webui"] == {"enabled": True, "port": 8765}


# --------------------------------------------------------------------------
# 3. _bundled_config_path
# --------------------------------------------------------------------------


def test_bundled_config_path_none_when_absent() -> None:
    """仓库未放置 config.json 时返回 None（批量导入为 opt-in）。"""
    assert _bundled_config_path() is None
