"""热重载接线单元测试（计划 todo 13）。

离线测试 :class:`ConfigReloader`（无 Star/AstrBot 依赖）：fake Scheduler /
fake Database / 临时配置文件 / 可控假时钟与假 sleep（不睡真实时间）。每个
用例在单个 ``asyncio.run`` 内完成。

覆盖计划验收点：

1. 防抖：200ms 内连续 5 次 request_rebuild 只产生一次实际重建。
2. 三态返回：文件损坏 → ``parse-failed``（任务未动）；配置一致 → ``no-op``；
   配置变更（订阅 / poll 设置 / 凭据）→ ``rebuilt`` 并带新订阅重建。
3. 任务触碰面：reloader 只经 scheduler.rebuild 触碰任务，绝不动
   watcher/维护任务（scheduler 调用面断言）。
4. 身份清理：被删订阅 / type-uid 变更订阅 → delete_sub_state + status 条目 +
   retry 计数条目全部清除（强制重 seed）。
5. save-then-swap：normalize 新分配 id → 先持久化再换 ``_active_config``
   （save 时刻快照仍为旧值 + 下一次比对稳定为 no-op）。
6. ``_closing`` → request_rebuild 直接返回 ``no-op``，不触碰任务。
7. watcher：文件变化触发重建；无变化跳过；损坏 JSON 保留旧快照下一 tick
   重试；no-op 后刷新快照（W1 回归：下一次 tick 不再触发）。
8. 重建期间的新请求合并到下一个防抖轮次（绝无并发重建）。
9. clear_disabled：no-op 时仍应用；突发合并取 OR。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Callable
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from config import Subscription
from main import (
    ConfigReloader,
    _identity_changed,
    _REBUILD_DEBOUNCE_SEC,
    _WATCH_INTERVAL_SEC,
)

_SESSION = "aiocqhttp:GroupMessage:123"


class FakeClock:
    """可控时钟：``tick(sec)`` 推进，``t`` 为当前秒数。"""

    def __init__(self, start: float = 1_700_000_000.0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def tick(self, sec: float) -> None:
        self.t += sec


class ControlledSleep:
    """受控假 sleep：记录时长并阻塞，直到测试推进时钟满足条件。"""

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.deltas: list[float] = []
        self._pending: list[tuple[float, asyncio.Event]] = []

    async def __call__(self, sec: float) -> None:
        self.deltas.append(sec)
        start = self.clock.t
        while self.clock.t - start < sec:
            event = asyncio.Event()
            self._pending.append((sec, event))
            await event.wait()

    async def advance(self, sec: float) -> None:
        """推进时钟并唤醒所有阻塞中的 sleep；不满足条件的会重新注册。"""
        self.clock.tick(sec)
        pending, self._pending = self._pending, []
        for _sec, event in pending:
            event.set()
        await asyncio.sleep(0)


class FakeDb:
    """记录 ``delete_sub_state`` 调用的假数据层。"""

    def __init__(self) -> None:
        self.deleted: list[str] = []

    async def delete_sub_state(self, sub_id: str) -> None:
        self.deleted.append(sub_id)


class FakeScheduler:
    """记录调用面的假 Scheduler：只实现 reloader 用到的表面。"""

    def __init__(self) -> None:
        self.called: list[str] = []
        self.rebuild_calls: list[
            tuple[list[Subscription], dict[str, Any] | None, bool]
        ] = []
        self.clear_calls: int = 0
        self.status: dict[str, Any] = {}
        self.retry_counts: dict[str, dict[str, int]] = {}
        self._credential_cfg: dict[str, Any] = {}
        self.in_flight: int = 0
        self.max_in_flight: int = 0
        #: 测试门控：非 None 时 rebuild 等待 release 后才返回（模拟慢重建）。
        self.rebuild_gate: asyncio.Event | None = None

    async def rebuild(
        self,
        new_subs: list[Subscription],
        new_poll_settings: dict[str, Any] | None = None,
        clear_disabled: bool = False,
    ) -> None:
        self.called.append("rebuild")
        self.rebuild_calls.append((list(new_subs), new_poll_settings, clear_disabled))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        if self.rebuild_gate is not None:
            await self.rebuild_gate.wait()
        self.in_flight -= 1

    def clear_disabled(self) -> None:
        self.called.append("clear_disabled")
        self.clear_calls += 1


class RecordHandler(logging.Handler):
    """收集 LogRecord 的 handler，用于断言告警路径。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


class PersistingWriter:
    """模拟 AstrBotConfig 的写入器：记录 save 并原子落盘。

    同时记录每次 save 时刻 ``_active_config`` 中的订阅 id 集——用于证明
    save-then-swap 顺序（save 时快照必须仍是旧值，尚未换新）。
    """

    def __init__(self, path: Path, reloader: ConfigReloader) -> None:
        self.path = path
        self.reloader = reloader
        self.saves: list[dict[str, Any]] = []
        self.active_ids_at_save: list[set[str] | None] = []

    async def save_config_async(
        self, replace_config: dict[str, Any] | None = None
    ) -> bool:
        active = self.reloader._active_config
        self.active_ids_at_save.append(
            {s["id"] for s in active["subscriptions"]}  # type: ignore[index]
            if active is not None
            else None
        )
        with open(self.path, "w", encoding="utf-8") as f:
            json.dump(replace_config or {}, f, ensure_ascii=False)
        self.saves.append(dict(replace_config or {}))
        return True


def _sub_dict(
    sub_id: str | None, type_: str = "live", uid: int = 1, **extra: Any
) -> dict[str, Any]:
    """构造订阅原始 dict（sub_id 为 None 时不含 id → normalize 分配新 id）。"""
    sub: dict[str, Any] = {
        "type": type_,
        "name": f"{type_}:{uid}",
        "uid": uid,
        "poll_interval_sec": 300,
        "enabled": True,
        "push_session_ids": [_SESSION],
    }
    if sub_id is not None:
        sub["id"] = sub_id
    sub.update(extra)
    return sub


def _write_config(
    path: Path,
    subs: list[dict[str, Any]],
    credential: dict[str, Any] | None = None,
    poll: dict[str, Any] | None = None,
    padding: str = "",
) -> None:
    """写配置文件；``padding`` 可改变字节数（同内容不同尺寸）。"""
    payload = {
        "credential": credential if credential is not None else {"sessdata": "old"},
        "poll": poll
        if poll is not None
        else {
            "global_min_interval_sec": 60,
            "poll_jitter_sec": 15,
            "push_title_change": True,
        },
        "webui": {"enabled": True, "host": "127.0.0.1", "port": 8765, "token": ""},
        "subscriptions": subs,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False) + padding, encoding="utf-8")


def _make_reloader(
    path: Path,
    scheduler: FakeScheduler,
    db: FakeDb | None = None,
    status: dict[str, Any] | None = None,
    retry: dict[str, dict[str, int]] | None = None,
    writer: Any = None,
    name: str = "reload",
) -> tuple[ConfigReloader, ControlledSleep]:
    clock = FakeClock()
    sleep = ControlledSleep(clock)
    reloader = ConfigReloader(
        config_path=path,
        scheduler=scheduler,
        db=db if db is not None else FakeDb(),
        status=status if status is not None else scheduler.status,
        retry_counts=retry if retry is not None else scheduler.retry_counts,
        config_writer=writer,
        logger=logging.getLogger(f"test.reload.{name}"),
        sleep=sleep,
    )
    return reloader, sleep


async def _drive(task: asyncio.Task[str], sleep: ControlledSleep) -> str:
    """推进假时钟直到 request_rebuild 任务完成（防抖窗口可多轮）。"""
    for _ in range(20):
        if task.done():
            break
        await sleep.advance(_REBUILD_DEBOUNCE_SEC)
    if not task.done():
        raise AssertionError("request_rebuild 未在预期防抖轮次内完成")
    return task.result()


async def _rebuild_once(
    reloader: ConfigReloader,
    sleep: ControlledSleep,
    clear_disabled: bool = False,
) -> str:
    """发起一次 request_rebuild 并驱动到完成。"""
    return await _drive(
        asyncio.create_task(reloader.request_rebuild(clear_disabled=clear_disabled)),
        sleep,
    )


async def _advance_until(
    sleep: ControlledSleep,
    condition: Callable[[], bool],
    ticks: float = _REBUILD_DEBOUNCE_SEC,
    max_iters: int = 80,
) -> None:
    """推进假时钟直到条件满足（watcher 测试用）。"""
    for _ in range(max_iters):
        if condition():
            return
        await sleep.advance(ticks)
    raise AssertionError("推进假时钟至条件满足超时")


# ----------------------------------------------------------------------
# 1. 防抖：200ms 内连续 5 次 request_rebuild 只产生一次实际重建
# ----------------------------------------------------------------------


def test_debounce_5_calls_in_window_produce_one_rebuild() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_debounce.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        tasks = [asyncio.create_task(reloader.request_rebuild()) for _ in range(5)]
        await asyncio.sleep(0)  # 让全部 5 个请求完成注册
        for _ in range(10):
            if all(t.done() for t in tasks):
                break
            await sleep.advance(_REBUILD_DEBOUNCE_SEC)
        results = [t.result() for t in tasks]

        assert results == ["rebuilt"] * 5
        assert len(scheduler.rebuild_calls) == 1
        # 5 个请求在防抖循环首个窗口检查前全部注册完毕（任务按创建顺序执行），
        # 因此窗口只等了 1 个 0.2s 就把全部请求合并为一次重建。
        assert sleep.deltas.count(_REBUILD_DEBOUNCE_SEC) == 1

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 2. 三态返回：parse-failed / no-op / rebuilt（含 settings 与凭据变更）
# ----------------------------------------------------------------------


def test_tri_state_parse_failed_keeps_tasks() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_parse.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        path.write_text("{broken json", encoding="utf-8")

        result = await _rebuild_once(reloader, sleep)

        assert result == "parse-failed"
        assert scheduler.called == []
        assert reloader._active_config is None

    asyncio.run(scenario())


def test_tri_state_identical_config_is_noop() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_noop.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        result = await _rebuild_once(reloader, sleep)

        assert result == "no-op"
        assert len(scheduler.rebuild_calls) == 1

    asyncio.run(scenario())


def test_tri_state_changed_subs_rebuilds_with_new_subs() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_changed.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        _write_config(path, [_sub_dict("a", "live", 1), _sub_dict("b", "dynamic", 2)])
        result = await _rebuild_once(reloader, sleep)

        assert result == "rebuilt"
        assert len(scheduler.rebuild_calls) == 2
        new_subs = scheduler.rebuild_calls[-1][0]
        assert sorted(s.id for s in new_subs) == ["a", "b"]

    asyncio.run(scenario())


def test_poll_settings_change_triggers_rebuild() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_settings.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        _write_config(
            path,
            [_sub_dict("a", "live", 1)],
            poll={
                "global_min_interval_sec": 120,
                "poll_jitter_sec": 15,
                "push_title_change": True,
            },
        )
        result = await _rebuild_once(reloader, sleep)

        assert result == "rebuilt"
        assert scheduler.rebuild_calls[-1][1] == {
            "global_min_interval_sec": 120,
            "poll_jitter_sec": 15,
            "push_title_change": True,
        }

    asyncio.run(scenario())


def test_credential_change_triggers_rebuild_and_updates_repo_cfg() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_credential.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)], credential={"sessdata": "old"})
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert scheduler._credential_cfg == {"sessdata": "old"}

        _write_config(path, [_sub_dict("a", "live", 1)], credential={"sessdata": "new"})
        result = await _rebuild_once(reloader, sleep)

        assert result == "rebuilt"
        # scheduler.rebuild 以 _credential_cfg 重建 repository → 凭据必须更新
        assert scheduler._credential_cfg == {"sessdata": "new"}

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 3. 任务触碰面：只经 scheduler.rebuild；watcher/维护任务绝不被触碰
# ----------------------------------------------------------------------


def test_only_rebuild_touches_tasks() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_tasks.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        # reloader 的任务触碰面只有 scheduler.rebuild（cancel 3 个轮询任务 +
        # await 由 scheduler.rebuild 内部完成）；绝无对 watcher/维护任务、
        # stop/任务级 API 的直接调用。
        assert scheduler.called == ["rebuild"]

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 4. 身份清理：被删 / type-uid 变更的订阅全量清库 + 清运行时状态
# ----------------------------------------------------------------------


def test_identity_cleanup_removed_and_changed() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_cleanup.json"
        scheduler = FakeScheduler()
        db = FakeDb()
        status = {"a": SimpleNamespace(), "b": SimpleNamespace()}
        retry = {"a": {"d1": 1}, "b": {"d2": 2}}
        reloader, sleep = _make_reloader(
            path, scheduler, db, status=status, retry=retry
        )
        _write_config(path, [_sub_dict("a", "live", 1), _sub_dict("b", "dynamic", 2)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert db.deleted == []
        assert set(status) == {"a", "b"}

        # 订阅 b 被删除 → 清库 + 清 status + 清 retry
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert db.deleted == ["b"]
        assert "b" not in status and "b" not in retry
        assert "a" in status  # 未受影响的订阅原样保留

        # 订阅 a type 变更（live → dynamic，同 id）→ 身份变化强制重 seed
        _write_config(path, [_sub_dict("a", "dynamic", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert db.deleted == ["b", "a"]
        assert "a" not in status and "a" not in retry

        # 订阅 a uid 变更（同 type）→ 同样视为身份变化
        status["a"] = SimpleNamespace()
        retry["a"] = {}
        _write_config(path, [_sub_dict("a", "dynamic", 999)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert db.deleted == ["b", "a", "a"]
        assert "a" not in status and "a" not in retry

    asyncio.run(scenario())


def test_identity_changed_detector() -> None:
    sub = Subscription(
        id="a",
        type="live",
        name="n",
        uid=1,
        poll_interval_sec=300,
        enabled=True,
        push_session_ids=[_SESSION],
    )
    assert not _identity_changed(_sub_dict("a", "live", 1), sub)
    assert _identity_changed(_sub_dict("a", "live", 2), sub)  # uid
    assert _identity_changed(_sub_dict("a", "dynamic", 1), sub)  # type
    collection = Subscription(
        id="c",
        type="collection",
        name="n",
        uid=1,
        list_id=10,
        series_type=0,
        poll_interval_sec=300,
        enabled=True,
        push_session_ids=[_SESSION],
    )
    assert not _identity_changed(
        _sub_dict("c", "collection", 1, list_id=10, series_type=0), collection
    )
    assert _identity_changed(
        _sub_dict("c", "collection", 1, list_id=11, series_type=0), collection
    )  # list_id
    assert _identity_changed(
        _sub_dict("c", "collection", 1, list_id=10, series_type=1), collection
    )  # series_type


# ----------------------------------------------------------------------
# 5. save-then-swap：normalize 新分配 id → 先持久化再换 _active_config
# ----------------------------------------------------------------------


def test_save_then_swap_ordering_and_stable_ids() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_save.json"
        scheduler = FakeScheduler()
        db = FakeDb()
        status: dict[str, Any] = {}
        retry: dict[str, dict[str, int]] = {}
        clock = FakeClock()
        sleep = ControlledSleep(clock)
        reloader = ConfigReloader(
            config_path=path,
            scheduler=scheduler,
            db=db,
            status=status,
            retry_counts=retry,
            logger=logging.getLogger("test.reload.save"),
            sleep=sleep,
        )
        # 先建立快照：带 id 的正常配置
        _write_config(path, [_sub_dict("a", "live", 1), _sub_dict("b", "live", 2)])
        assert (
            await _drive(asyncio.create_task(reloader.request_rebuild()), sleep)
            == "rebuilt"
        )
        assert len(scheduler.rebuild_calls) == 1

        # 手动编辑：新增一个无 id 的订阅 c → normalize 会分配新 id
        _write_config(
            path,
            [
                _sub_dict("a", "live", 1),
                _sub_dict("b", "live", 2),
                _sub_dict(None, "live", 3),
            ],
        )
        writer = PersistingWriter(path, reloader)
        reloader._config_writer = writer
        result = await _drive(asyncio.create_task(reloader.request_rebuild()), sleep)

        assert result == "rebuilt"
        assert len(writer.saves) == 1
        rebuilt_ids = {s.id for s in scheduler.rebuild_calls[-1][0]}
        saved_ids = {s["id"] for s in writer.saves[0]["subscriptions"]}
        # c 获得了新 id；重建订阅 id 与持久化 id 完全一致
        assert len(rebuilt_ids) == 3 and {"a", "b"} <= rebuilt_ids
        assert saved_ids == rebuilt_ids
        # save-then-swap：save 发生时 _active_config 仍为旧快照（未含新 id）
        assert writer.active_ids_at_save == [{"a", "b"}]
        # 新 id 已持久化到磁盘
        disk = json.loads(path.read_text(encoding="utf-8"))
        assert {s["id"] for s in disk["subscriptions"]} == saved_ids
        # 下一次比对稳定：文件（已含新 id）与 _active_config 一致 → no-op
        result = await _drive(asyncio.create_task(reloader.request_rebuild()), sleep)
        assert result == "no-op"
        assert len(scheduler.rebuild_calls) == 2

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 6. _closing：直接返回 no-op，不触碰任务
# ----------------------------------------------------------------------


def test_closing_returns_noop_without_touching_tasks() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_closing.json"
        scheduler = FakeScheduler()
        reloader, _sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        reloader._closing = True

        result = await reloader.request_rebuild(clear_disabled=True)

        assert result == "no-op"
        assert scheduler.called == []
        assert reloader._loop_task is None  # 未创建防抖任务

    asyncio.run(scenario())


def test_shutdown_stops_watcher_and_debounce() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_shutdown.json"
        scheduler = FakeScheduler()
        reloader, _sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        watcher = reloader.start_watcher()
        await asyncio.sleep(0)

        await reloader.shutdown()

        assert watcher.cancelled()
        assert reloader._closing
        assert await reloader.request_rebuild() == "no-op"

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 7. watcher：变化触发 / 无变化跳过 / 损坏保留旧快照重试 / no-op 刷新快照
# ----------------------------------------------------------------------


def test_watcher_no_change_skips_ticks() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_watcher_skip.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        watcher = reloader.start_watcher()
        try:
            await asyncio.sleep(0)  # watcher 完成首帧快照并注册首个 5s 睡眠

            await sleep.advance(_WATCH_INTERVAL_SEC)  # tick 1：未变化 → 跳过
            await sleep.advance(_WATCH_INTERVAL_SEC)  # tick 2：仍未变化 → 跳过

            assert scheduler.called == []
            assert sleep.deltas.count(_WATCH_INTERVAL_SEC) >= 2  # 两个真实 tick
        finally:
            await reloader.shutdown()
            assert watcher.cancelled()

    asyncio.run(scenario())


def test_watcher_file_change_triggers_rebuild() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_watcher_change.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        reloader.start_watcher()
        try:
            await asyncio.sleep(0)

            _write_config(
                path, [_sub_dict("a", "live", 1), _sub_dict("b", "dynamic", 2)]
            )
            await sleep.advance(_WATCH_INTERVAL_SEC)  # tick：检测到变化
            await _advance_until(sleep, lambda: len(scheduler.rebuild_calls) >= 1)

            assert len(scheduler.rebuild_calls) == 1
            assert sorted(s.id for s in scheduler.rebuild_calls[0][0]) == ["a", "b"]

            # 快照已刷新 → 下一次 tick 不再触发
            await sleep.advance(_WATCH_INTERVAL_SEC)
            assert len(scheduler.rebuild_calls) == 1
        finally:
            await reloader.shutdown()

    asyncio.run(scenario())


def test_watcher_parse_failure_keeps_snapshot_and_retries() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_watcher_corrupt.json"
        logger = logging.getLogger("test.reload.corrupt")
        logger.handlers.clear()
        logger.setLevel(logging.WARNING)
        handler = RecordHandler()
        logger.addHandler(handler)
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler, name="corrupt")
        _write_config(path, [_sub_dict("a", "live", 1)])
        reloader.start_watcher()
        try:
            await asyncio.sleep(0)

            # 撕裂写：损坏的 JSON（尺寸/时间戳变化）
            path.write_text("{not valid json", encoding="utf-8")
            await sleep.advance(_WATCH_INTERVAL_SEC)
            await _advance_until(sleep, lambda: len(handler.records) >= 1)

            assert scheduler.called == []  # 解析失败 → 任务未动
            assert any("重读配置" in r.getMessage() for r in handler.records)

            # 快照保留旧值 → 修复文件后下一 tick 重试并成功
            _write_config(
                path, [_sub_dict("a", "live", 1), _sub_dict("b", "dynamic", 2)]
            )
            await sleep.advance(_WATCH_INTERVAL_SEC)
            await _advance_until(sleep, lambda: len(scheduler.rebuild_calls) >= 1)

            assert len(scheduler.rebuild_calls) == 1
            assert sorted(s.id for s in scheduler.rebuild_calls[0][0]) == ["a", "b"]
        finally:
            await reloader.shutdown()

    asyncio.run(scenario())


def test_watcher_noop_refreshes_snapshot_no_reloop() -> None:
    """W1 回归：WebUI 保存（同内容重写）→ watcher 下一 tick 不得再触发。"""

    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_watcher_w1.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        reloader.start_watcher()
        try:
            await asyncio.sleep(0)

            # 首次重建：_active_config 与文件一致
            assert (
                await _drive(asyncio.create_task(reloader.request_rebuild()), sleep)
                == "rebuilt"
            )
            assert len(scheduler.rebuild_calls) == 1

            # 模拟 WebUI 保存：同内容、不同字节（尺寸变化）→ watcher 感知变化
            _write_config(path, [_sub_dict("a", "live", 1)], padding="\n\n  ")
            await sleep.advance(_WATCH_INTERVAL_SEC)  # tick 1：检测到变化
            # 驱动到 watcher 完成本轮（重新注册下一个 5s 睡眠 = 第 2 个 5s delta）
            await _advance_until(
                sleep, lambda: sleep.deltas.count(_WATCH_INTERVAL_SEC) >= 2
            )
            # no-op 轮次：未触发重建（快照等于 _active_config）
            assert len(scheduler.rebuild_calls) == 1

            # tick 2：no-op 已刷新快照 → 不再触发（若未刷新则会再次 request）
            await sleep.advance(_WATCH_INTERVAL_SEC)
            await _advance_until(
                sleep, lambda: sleep.deltas.count(_WATCH_INTERVAL_SEC) >= 3
            )
            assert len(scheduler.rebuild_calls) == 1
        finally:
            await reloader.shutdown()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 8. 重建期间的新请求合并到下一轮（绝无并发重建）
# ----------------------------------------------------------------------


def test_request_during_inflight_rebuild_coalesces_next_pass() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_inflight.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        # 第一轮：正常完成（建立 _active_config）
        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        # 第二轮：慢重建（门控）——模拟重建进行中
        _write_config(path, [_sub_dict("a", "live", 1), _sub_dict("b", "live", 2)])
        scheduler.rebuild_gate = asyncio.Event()
        first = asyncio.create_task(reloader.request_rebuild())
        await _advance_until(sleep, lambda: len(scheduler.rebuild_calls) >= 2)
        assert scheduler.in_flight == 1  # 重建 #2 正在跑

        # 重建期间的新请求：合并到下一个防抖轮次，不并发
        second = asyncio.create_task(reloader.request_rebuild())
        await asyncio.sleep(0)
        assert len(scheduler.rebuild_calls) == 2  # 未启动第二个并发重建
        assert scheduler.in_flight == 1

        # 放行重建 #2 → 其后的合并轮次再处理一次（文件已一致 → no-op）
        scheduler.rebuild_gate.set()
        result_first = await first
        await _advance_until(sleep, lambda: second.done())

        assert result_first == "rebuilt"
        assert second.result() == "no-op"
        assert len(scheduler.rebuild_calls) == 2
        assert scheduler.max_in_flight == 1  # 任意时刻至多一个重建在跑

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 9. clear_disabled：no-op 时仍应用；突发合并取 OR
# ----------------------------------------------------------------------


def test_clear_disabled_applied_on_noop() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_clear.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert scheduler.clear_calls == 0

        # 配置一致（no-op）但 clear_disabled=True → 仍清标志（廉价、无任务搅动）
        result = await _rebuild_once(reloader, sleep, clear_disabled=True)
        assert result == "no-op"
        assert scheduler.clear_calls == 1
        assert len(scheduler.rebuild_calls) == 1

    asyncio.run(scenario())


def test_clear_disabled_or_merge_in_burst() -> None:
    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_or.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        tasks = [
            asyncio.create_task(reloader.request_rebuild()),
            asyncio.create_task(reloader.request_rebuild(clear_disabled=True)),
        ]
        await asyncio.sleep(0)
        for _ in range(10):
            if all(t.done() for t in tasks):
                break
            await sleep.advance(_REBUILD_DEBOUNCE_SEC)
        results = [t.result() for t in tasks]

        assert results == ["rebuilt", "rebuilt"]
        assert len(scheduler.rebuild_calls) == 1  # 突发合并为一次重建
        assert scheduler.rebuild_calls[0][2] is True  # clear_disabled 取 OR

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 10. UTF-8 BOM 兼容 + 失败去重 + config_status
# ----------------------------------------------------------------------


def test_read_config_with_utf8_bom(tmp_path: Path) -> None:
    """AstrBotConfig 以 utf-8-sig 落盘（带 BOM）也能被热重载读取。"""

    async def scenario() -> None:
        path = tmp_path / "bom.json"
        payload = {
            "credential": {"sessdata": "abc"},
            "poll": {"global_min_interval_sec": 60, "poll_jitter_sec": 0},
            "webui": {"enabled": False},
            "subscriptions": [_sub_dict("a", "live", 1)],
        }
        with open(path, "w", encoding="utf-8-sig") as f:
            json.dump(payload, f, ensure_ascii=False)
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)

        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert len(scheduler.rebuild_calls) == 1
        assert reloader.config_status()["ok"] is True

    asyncio.run(scenario())


def test_read_failure_warning_deduped() -> None:
    """连续同一读取失败只告警一次（watcher 每 5s 重试不再刷屏）。"""

    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_dedup_warn.json"
        logger = logging.getLogger("test.reload.dedup_warn")
        logger.handlers.clear()
        logger.setLevel(logging.WARNING)
        handler = RecordHandler()
        logger.addHandler(handler)
        scheduler = FakeScheduler()
        reloader, _sleep = _make_reloader(path, scheduler)
        reloader._logger = logger
        path.write_text("{broken json", encoding="utf-8")

        for _ in range(3):
            assert reloader._read_and_normalize() is None

        warns = [
            r.getMessage() for r in handler.records if "重读配置" in r.getMessage()
        ]
        assert len(warns) == 1
        assert reloader.config_status()["ok"] is False
        assert "读取失败" in reloader.config_status()["last_error"]

    asyncio.run(scenario())


def test_config_status_recovers_after_success() -> None:
    """读取失败后 config_status.ok=False；修复文件后恢复 ok=True 且清空 last_error。"""

    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_cfg_status.json"
        scheduler = FakeScheduler()
        reloader, _sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])

        assert await _rebuild_once(reloader, _sleep) == "rebuilt"
        assert reloader.config_status()["ok"] is True
        assert reloader.config_status()["last_error"] is None

        path.write_text("{broken", encoding="utf-8")
        assert reloader._read_and_normalize() is None
        assert reloader.config_status()["ok"] is False
        assert reloader.config_status()["last_error"]

        _write_config(path, [_sub_dict("a", "live", 1)])
        assert reloader._read_and_normalize() is not None
        assert reloader.config_status()["ok"] is True
        assert reloader.config_status()["last_error"] is None

    asyncio.run(scenario())


def test_shutdown_then_reset_restores_hot_reload() -> None:
    """shutdown 后 reset()：request_rebuild 恢复可用（不再永远 no-op）。"""

    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_reset.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        await reloader.shutdown()
        assert await reloader.request_rebuild() == "no-op"  # 关闭后拒绝重建

        reloader.reset()
        _write_config(path, [_sub_dict("a", "live", 1), _sub_dict("b", "dynamic", 2)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"
        assert len(scheduler.rebuild_calls) == 2

    asyncio.run(scenario())


def test_missing_subscriptions_key_rejected_not_wiped() -> None:
    """配置文件缺 subscriptions 键（手改笔误）→ 重建被拒，绝不清空订阅。"""

    async def scenario() -> None:
        path = Path(__file__).parent / "tmp_nosubs.json"
        scheduler = FakeScheduler()
        reloader, sleep = _make_reloader(path, scheduler)
        _write_config(path, [_sub_dict("a", "live", 1)])
        assert await _rebuild_once(reloader, sleep) == "rebuilt"

        # 模拟手改笔误：删掉 subscriptions 键
        path.write_text(
            json.dumps(
                {
                    "credential": {},
                    "poll": {"global_min_interval_sec": 60, "poll_jitter_sec": 0},
                    "webui": {},
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        assert await _rebuild_once(reloader, sleep) == "parse-failed"
        assert len(scheduler.rebuild_calls) == 1  # 未触发第二次重建（订阅未被清空）
        status = reloader.config_status()
        assert status["ok"] is False
        assert "subscriptions" in status["last_error"]

    asyncio.run(scenario())
