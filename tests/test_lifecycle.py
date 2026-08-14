"""生命周期接线单元测试（计划 todo 14）。

离线测试 :class:`PluginLifecycle`（无 Star/AstrBot 依赖）：fake context /
fake Database / fake Scheduler / fake WebUIServer / fake ConfigReloader，
全组件注入驱动 ``initialize()`` / ``terminate()`` 的顺序与鲁棒性；重载循环
用例用真实 :class:`ConfigReloader`（临时配置文件）验证身份变更清理。每个
用例在单个 ``asyncio.run`` 内完成（无 pytest-asyncio）。

覆盖计划验收点：

1. initialize 顺序：db.init → webui.start（启用时）→ scheduler.start →
   维护任务创建 → watcher 启动；webui 禁用时不启动。
2. terminate 顺序：_closing 先置位 → watcher 先停（阻止新重建）→ 重建锁内
   scheduler.stop → webui.stop → db.close。
3. terminate 幂等：重复调用 / 组件未启动时不崩溃。
4. ``asyncio.CancelledError`` 在 terminate 中自然传播（不吞、不掩盖后续步骤）。
5. WebUI 启动失败（端口占用）→ 记日志 + 禁用，scheduler 照常启动（降级）。
6. status dict 共享：lifecycle.status 与 scheduler.status 是同一个 dict。
7. 重载循环：request_rebuild 三态返回 + 启动快照 no-op；身份变更触发
   db.delete_sub_state（强制重 seed）。
8. 无 sessdata → 匿名模式告警，仍继续初始化；有 sessdata → 无告警。
"""

from __future__ import annotations

import asyncio
import copy
import json
import logging
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from config import normalize
from main import ConfigReloader, PluginLifecycle

_SESSION = "aiocqhttp:GroupMessage:123"


class _RecordListHandler(logging.Handler):
    """收集 LogRecord 的内存 handler（断言日志用）。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class FakeContext:
    """duck context：记录 send_message 调用。"""

    def __init__(self) -> None:
        self.sent: list[tuple[str, Any]] = []

    async def send_message(self, session: str, chain: Any) -> bool:
        self.sent.append((session, chain))
        return True


class FakeDb:
    """记录 init/close/delete_sub_state 调用的假数据层。"""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.deleted: list[str] = []

    async def init(self) -> None:
        self.events.append("db.init")

    async def close(self) -> None:
        self.events.append("db.close")

    async def delete_sub_state(self, sub_id: str) -> None:
        self.deleted.append(sub_id)


class FakeScheduler:
    """记录生命周期调用面 + 供 reloader 重建的假 Scheduler。"""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.status: dict[str, Any] = {}
        self.retry_counts: dict[str, dict[str, int]] = {}
        self._credential_cfg: dict[str, Any] = {}
        self.rebuild_calls: list[tuple[list[Any], dict[str, Any] | None, bool]] = []
        self.clear_calls: int = 0
        self._maintenance_task: asyncio.Task[None] | None = None

    def start(self) -> None:
        self.events.append("sched.start")

    async def stop(self) -> None:
        self.events.append("sched.stop")
        if self._maintenance_task is not None and not self._maintenance_task.done():
            self._maintenance_task.cancel()
            await asyncio.gather(self._maintenance_task, return_exceptions=True)

    def create_maintenance_task(self) -> asyncio.Task[None]:
        self.events.append("sched.maintenance")
        self._maintenance_task = asyncio.create_task(self._noop_loop())
        return self._maintenance_task

    async def _noop_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise

    async def rebuild(
        self,
        new_subs: list[Any],
        new_poll_settings: dict[str, Any] | None = None,
        clear_disabled: bool = False,
    ) -> None:
        self.rebuild_calls.append((list(new_subs), new_poll_settings, clear_disabled))

    def clear_disabled(self) -> None:
        self.clear_calls += 1


class FakeWebUI:
    """记录 start/stop/install/remove 调用面的假 WebUI。"""

    def __init__(self, events: list[str], fail_start: bool = False) -> None:
        self.events = events
        self.enabled = True
        self.started_with: tuple[str, int] | None = None
        self._fail_start = fail_start

    async def start(self, host: str, port: int) -> None:
        self.events.append("webui.start")
        if self._fail_start:
            raise RuntimeError("address already in use")
        self.started_with = (host, port)

    async def stop(self) -> None:
        self.events.append("webui.stop")

    def install_log_handler(self) -> None:
        self.events.append("webui.install")

    def remove_log_handler(self) -> None:
        self.events.append("webui.remove")


class FakeReloader:
    """记录生命周期调用面并托管 watcher 任务的假 reloader。"""

    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.lock = asyncio.Lock()
        self.watcher_task: asyncio.Task[None] | None = None
        #: 关闭探针：shutdown 开始瞬间回调（断言 _closing 先置位）。
        self._closing_probe: Any = None

    def start_watcher(self) -> asyncio.Task[None]:
        self.events.append("reloader.watcher")
        self.watcher_task = asyncio.create_task(self._noop_loop())
        return self.watcher_task

    async def _noop_loop(self) -> None:
        try:
            while True:
                await asyncio.sleep(3600)
        except asyncio.CancelledError:
            raise

    async def shutdown(self) -> None:
        if self._closing_probe is not None:
            self.events.append(f"closing={self._closing_probe()}")
        self.events.append("reloader.shutdown")
        if self.watcher_task is not None and not self.watcher_task.done():
            self.watcher_task.cancel()
            await asyncio.gather(self.watcher_task, return_exceptions=True)

    async def request_rebuild(self, clear_disabled: bool = False) -> str:
        del clear_disabled
        return "no-op"


def _make_logger() -> tuple[logging.Logger, _RecordListHandler]:
    """返回隔离的测试 logger + 记录 handler。"""
    handler = _RecordListHandler()
    logger = logging.getLogger("test_lifecycle")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)
    logger.propagate = False
    return logger, handler


def _make_config(webui_enabled: bool = True, **webui_extra: Any) -> dict[str, Any]:
    """构造承载四配置组的假 AstrBotConfig（dict 形态）。"""
    webui = {"enabled": webui_enabled, "host": "127.0.0.1", "port": 8765}
    webui.update(webui_extra)
    return {
        "credential": {"sessdata": "abc"},
        "poll": {"global_min_interval_sec": 60, "poll_jitter_sec": 0},
        "webui": webui,
        "subscriptions": [],
    }


class LifecycleHarness:
    """组装 lifecycle 与全部 fake 记录对象的夹具。"""

    def __init__(
        self,
        config: dict[str, Any] | None = None,
        *,
        webui: Any | None = None,
        fail_webui: bool = False,
        credential_cfg: dict[str, Any] | None = None,
    ) -> None:
        self.events: list[str] = []
        self.config = config if config is not None else _make_config()
        self.context = FakeContext()
        self.logger, self.handler = _make_logger()
        self.db = FakeDb(self.events)
        self.scheduler = FakeScheduler(self.events)
        self.reloader = FakeReloader(self.events)
        if webui is not None:
            self.webui: Any = webui
        elif bool((self.config.get("webui") or {}).get("enabled", False)):
            self.webui = FakeWebUI(self.events, fail_start=fail_webui)
        else:
            self.webui = None
        self.lifecycle = PluginLifecycle(
            config=self.config,
            context=self.context,
            credential_cfg=credential_cfg
            if credential_cfg is not None
            else {"sessdata": "abc"},
            db=self.db,
            scheduler=self.scheduler,
            reloader=self.reloader,
            logger=self.logger,
            webui=self.webui,
        )


def test_initialize_order_with_webui() -> None:
    """启用 WebUI：db.init → webui.start(host, port) → install → scheduler
    → 维护任务 → watcher。"""

    async def _case() -> None:
        harness = LifecycleHarness()
        await harness.lifecycle.initialize()
        assert harness.events == [
            "db.init",
            "webui.start",
            "webui.install",
            "sched.start",
            "sched.maintenance",
            "reloader.watcher",
        ]
        assert harness.webui.started_with == ("127.0.0.1", 8765)
        assert harness.lifecycle._watcher_task is not None
        assert not harness.lifecycle._watcher_task.done()
        assert harness.lifecycle._maintenance_task is not None
        assert not harness.lifecycle._maintenance_task.done()

    asyncio.run(_case())


def test_initialize_webui_disabled() -> None:
    """禁用 WebUI：不构建、不启动，其余顺序不变。"""

    async def _case() -> None:
        harness = LifecycleHarness(_make_config(webui_enabled=False))
        assert harness.webui is None
        await harness.lifecycle.initialize()
        assert harness.lifecycle.webui is None
        assert harness.events == [
            "db.init",
            "sched.start",
            "sched.maintenance",
            "reloader.watcher",
        ]

    asyncio.run(_case())


def test_terminate_order() -> None:
    """关停顺序：_closing 先置位 → watcher 先停 → scheduler → webui → db。"""

    async def _case() -> None:
        harness = LifecycleHarness()
        harness.reloader._closing_probe = lambda: harness.lifecycle._closing
        await harness.lifecycle.initialize()
        harness.events.clear()
        await harness.lifecycle.terminate()
        assert harness.lifecycle._closing is True
        assert harness.events == [
            "closing=True",
            "reloader.shutdown",
            "sched.stop",
            "webui.stop",
            "db.close",
        ]
        assert harness.reloader.watcher_task.cancelled()

    asyncio.run(_case())


def test_terminate_idempotent() -> None:
    """terminate 重复调用不崩溃（组件已停）。"""

    async def _case() -> None:
        harness = LifecycleHarness()
        await harness.lifecycle.initialize()
        await harness.lifecycle.terminate()
        await harness.lifecycle.terminate()

    asyncio.run(_case())


def test_terminate_without_initialize() -> None:
    """组件未启动时直接 terminate 也安全（各组件幂等）。"""

    async def _case() -> None:
        harness = LifecycleHarness()
        await harness.lifecycle.terminate()
        assert harness.events == [
            "reloader.shutdown",
            "sched.stop",
            "webui.stop",
            "db.close",
        ]

    asyncio.run(_case())


def test_cancelled_error_propagates() -> None:
    """scheduler.stop 被取消时 CancelledError 自然传播，后续步骤不执行。"""

    async def _case() -> None:
        harness = LifecycleHarness()

        async def _raising_stop() -> None:
            harness.events.append("sched.stop")
            raise asyncio.CancelledError

        harness.scheduler.stop = _raising_stop  # type: ignore[method-assign]
        await harness.lifecycle.initialize()
        with pytest.raises(asyncio.CancelledError):
            await harness.lifecycle.terminate()
        assert harness.lifecycle._closing is True
        assert "webui.stop" not in harness.events
        assert "db.close" not in harness.events

    asyncio.run(_case())


def test_webui_start_failure_degrades() -> None:
    """WebUI 启动失败：记 error + 禁用 + 不装日志 Handler，scheduler 照常。"""

    async def _case() -> None:
        harness = LifecycleHarness(fail_webui=True)
        await harness.lifecycle.initialize()
        assert harness.lifecycle.webui is not None
        assert harness.lifecycle.webui.enabled is False
        assert "webui.install" not in harness.events
        assert "sched.start" in harness.events
        assert "reloader.watcher" in harness.events
        errors = [
            r.getMessage()
            for r in harness.handler.records
            if r.levelno == logging.ERROR
        ]
        assert any("WebUI 启动失败" in message for message in errors)

    asyncio.run(_case())


def test_status_dict_shared() -> None:
    """lifecycle.status 与 scheduler.status 是同一个 dict。"""

    async def _case() -> None:
        harness = LifecycleHarness()
        await harness.lifecycle.initialize()
        assert harness.lifecycle.status is harness.scheduler.status
        harness.scheduler.status["sub-1"] = SimpleNamespace(last_error=None)
        assert harness.lifecycle.status["sub-1"].last_error is None

    asyncio.run(_case())


def test_rebuild_loop_real_reloader(tmp_path: Path) -> None:
    """真实 ConfigReloader：启动快照 no-op；身份变更 rebuilt + 清库重 seed。"""
    boot = {
        "credential": {"sessdata": "abc"},
        "poll": {"global_min_interval_sec": 60, "poll_jitter_sec": 0},
        "webui": {"enabled": False},
        "subscriptions": [
            {
                "id": "sub-1",
                "type": "live",
                "name": "A",
                "uid": 1,
                "enabled": True,
                "push_session_ids": [_SESSION],
            }
        ],
    }
    config_path = tmp_path / "astrbot_plugin_bilibili_cj_config.json"
    config_path.write_text(json.dumps(boot, ensure_ascii=False), encoding="utf-8")

    async def _case() -> None:
        events: list[str] = []
        logger, _handler = _make_logger()
        db = FakeDb(events)
        scheduler = FakeScheduler(events)
        status: dict[str, Any] = {}
        retry_counts: dict[str, dict[str, int]] = {}
        reloader = ConfigReloader(
            config_path=config_path,
            scheduler=scheduler,
            db=db,
            status=status,
            retry_counts=retry_counts,
            logger=logger,
        )
        reloader.seed_active_config(copy.deepcopy(boot), normalize(copy.deepcopy(boot)))
        lifecycle = PluginLifecycle(
            config=copy.deepcopy(boot),
            context=FakeContext(),
            credential_cfg={"sessdata": "abc"},
            db=db,
            scheduler=scheduler,
            reloader=reloader,
            logger=logger,
            webui=None,
        )
        await lifecycle.initialize()
        assert lifecycle.status is scheduler.status
        # 与启动快照一致 → no-op，不触碰任务。
        assert await reloader.request_rebuild() == "no-op"
        assert scheduler.rebuild_calls == []
        # 身份变更（live → dynamic，id 保留）→ rebuilt + 清库强制重 seed。
        changed = copy.deepcopy(boot)
        changed["subscriptions"][0]["type"] = "dynamic"
        config_path.write_text(
            json.dumps(changed, ensure_ascii=False), encoding="utf-8"
        )
        assert await reloader.request_rebuild(clear_disabled=True) == "rebuilt"
        assert db.deleted == ["sub-1"]
        assert scheduler.rebuild_calls[-1][0][0].type == "dynamic"
        assert scheduler.rebuild_calls[-1][2] is True  # clear_disabled=True
        assert reloader._active_config is not None  # 快照已更新（save-then-swap）
        await lifecycle.terminate()

    asyncio.run(_case())


def test_no_sessdata_anonymous_warning() -> None:
    """无 sessdata：告警匿名模式，仍继续启动。"""

    async def _case() -> None:
        harness = LifecycleHarness(credential_cfg={})
        await harness.lifecycle.initialize()
        warnings_ = [
            r.getMessage()
            for r in harness.handler.records
            if r.levelno == logging.WARNING
        ]
        assert warnings_
        assert any("sessdata" in message and "匿名" in message for message in warnings_)
        assert "sched.start" in harness.events

    asyncio.run(_case())


def test_sessdata_present_no_warning() -> None:
    """有 sessdata：不产生匿名告警。"""

    async def _case() -> None:
        harness = LifecycleHarness(credential_cfg={"sessdata": "abc"})
        await harness.lifecycle.initialize()
        warnings_ = [
            r.getMessage()
            for r in harness.handler.records
            if r.levelno == logging.WARNING
        ]
        assert all("sessdata" not in message for message in warnings_)

    asyncio.run(_case())
