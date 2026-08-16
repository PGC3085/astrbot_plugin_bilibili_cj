"""调度器单元测试（计划 todo 10）。

全部离线、**无真实 I/O**：fake repository、即时假数据层（``InstantDb``，
无 aiosqlite 线程往返，调度时序全确定性）、记录式 fake context、真实
``push.build_chain``/``push.send``（离线 str 模式）、注入可控时钟与假 sleep
（自动推进或受控推进，不睡真实时间）。每个用例在单个 ``asyncio.run`` 内完成。

覆盖计划验收点：

1. 令牌桶：容量 3、速率 0.5/s → 3 次立即取牌、第 4 次阻塞至补充（假时钟）；
   补充累积并按容量封顶。2. 错误退避序列 2^n、上限 5min。3. 连续失败 10 次
   自动禁用 + 告警推送；第 11 轮整轮跳过；status.auto_disabled。4. 逐订阅
   间隔（两个不同间隔的 live 订阅 → 睡眠序列 [10,20,10,20,...]）。5. 重建：
   旧任务取消、新 poller 构建、status/auto-disable 保留、clear_disabled 清空。
   6. 限速：per-poll 令牌（合集多页轮询不再页间阻塞）+ 桶容量 3 时第 4 个
   订阅阻塞（假 sleep 受控模式）。7. stop() 干净取消全部任务。8. 维护任务
   每 6h 调一次 db.prune_old。9. 下播确认中的快速复查（短间隔不等满轮询
间隔）与 ``last_poll`` 在观测完成后写入。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable
from types import SimpleNamespace
from typing import Any

import pytest

from config import Subscription
from push import build_chain, send
from repository import BiliNetworkError
from scheduler import Scheduler, TokenBucket

_SESSION = "aiocqhttp:GroupMessage:123"
_T0 = 1_700_000_000
#: 空闲睡眠哨兵：非 idle 过滤阈值（_IDLE_SLEEP_SEC = 3600）。
_IDLE = 3600.0
#: 驱动循环迭代上限，防条件永不满足时死循环。
_MAX_ITERS = 50_000


class FakeClock:
    """可控时钟：``tick(sec)`` 推进，``t`` 为当前秒数。"""

    def __init__(self, start: float = _T0) -> None:
        self.t = float(start)

    def __call__(self) -> float:
        return self.t

    def tick(self, sec: float) -> None:
        self.t += sec


class AutoSleep:
    """自动推进假 sleep：记录每次请求的时长并推进时钟，随后让出一次。

    模拟"睡眠到期"：调用方（调度器/桶）无需外部驱动即自动推进；测试通过
    记录的 delta 序列断言调度行为。空闲睡眠（≥3600）被 :meth:`non_idle`
    过滤掉。
    """

    def __init__(self, clock: FakeClock) -> None:
        self.clock = clock
        self.deltas: list[float] = []

    async def __call__(self, sec: float) -> None:
        self.deltas.append(sec)
        self.clock.tick(sec)
        await asyncio.sleep(0)

    def non_idle(self) -> list[float]:
        return [d for d in self.deltas if d < _IDLE]


class ControlledSleep:
    """受控假 sleep：记录时长并阻塞，直到测试推进时钟满足条件。

    用于断言"请求在桶空时确实阻塞、时钟推进后才继续"。
    """

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


class InstantDb:
    """即时假数据层：无真实 I/O（无 aiosqlite 线程往返），时序全确定性。

    仅实现调度器/轮询器实际用到的方法；真实 SQLite 路径由既有轮询器测试
    覆盖，调度器测试只关心编排时序。
    """

    def __init__(self) -> None:
        self._live: dict[str, Any] = {}
        self.seeded: set[str] = set()
        self.seen: set[tuple[str, str]] = set()

    async def get_live_state(self, sub_id: str) -> Any | None:
        return self._live.get(sub_id)

    async def upsert_live_state(self, sub_id: str, **fields: Any) -> None:
        state = self._live.get(sub_id)
        if state is None:
            # 与 db.LiveState 字段形状一致（含 pending_push 等），
            # 缺省 None，避免轮询器读取缺失属性。
            state = SimpleNamespace(
                sub_id=sub_id,
                uid=None,
                room_id=None,
                last_status=None,
                last_title=None,
                last_live_time=None,
                live_ended_at=None,
                consecutive_offline_count=None,
                last_checked_at=None,
                pending_push=None,
                offline_notified=None,
            )
            self._live[sub_id] = state
        for key, value in fields.items():
            setattr(state, key, value)

    async def get_seeded(self, table: str, sub_id: str) -> bool:
        del table
        return sub_id in self.seeded

    async def set_seeded(self, table: str, sub_id: str, value: bool) -> None:
        del table
        if value:
            self.seeded.add(sub_id)
        else:
            self.seeded.discard(sub_id)

    async def insert_video_if_new(
        self, sub_id: str, bvid: str, uid: int, list_id: int
    ) -> bool:
        del uid, list_id
        key = (sub_id, bvid)
        if key in self.seen:
            return False
        self.seen.add(key)
        return True

    async def insert_dynamic_if_new(
        self, sub_id: str, dynamic_id: str, type_: int
    ) -> bool:
        del type_
        return await self.insert_video_if_new(sub_id, dynamic_id, 0, 0)

    async def prune_old(self) -> None:
        return None


def _room(status: int = 0, title: str = "测试标题", start: int = _T0) -> dict:
    return {
        "live_status": status,
        "title": title,
        "live_start_time": start,
        "area_name": "游戏",
        "cover": "https://example.com/cover.jpg",
    }


class LiveFakeRepo:
    """脚本化 live 仓库：按 uid 计数 get_live_info；错误标志持续抛出。"""

    def __init__(self, room: dict | None = None, roomid: int = 1) -> None:
        self.roomid = roomid
        self.room = dict(room or _room(0))
        self.room_info_calls = 0
        self.live_info_calls: dict[int, int] = {}
        self.live_error: Exception | None = None
        self.room_error: Exception | None = None

    async def get_live_info(self, uid: int) -> dict:
        self.live_info_calls[uid] = self.live_info_calls.get(uid, 0) + 1
        if self.live_error is not None:
            raise self.live_error
        return {"live_room": {"roomid": self.roomid}}

    async def get_room_info(self, room_id: int) -> dict:
        self.room_info_calls += 1
        if self.room_error is not None:
            raise self.room_error
        return dict(self.room)


class CollectionFakeRepo:
    """脚本化合集仓库：按 ``ps`` 切片服务一个扁平 item 列表并计数调用。"""

    def __init__(self, items: list[dict]) -> None:
        self.items = list(items)
        self.calls = 0

    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict:
        del uid, list_id, series_type
        self.calls += 1
        start = (pn - 1) * ps
        return {
            "archives": self.items[start : start + ps],
            "meta": {"name": "测试合集"},
        }


class FakeContext:
    """记录 ``send_message`` 调用；默认全部投递成功。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.sent: list[tuple[str, object]] = []

    async def send_message(self, session: str, chain: object) -> bool:
        self.sent.append((session, chain))
        return self.ok


def _live_sub(
    sub_id: str, uid: int, interval: int = 300, enabled: bool = True
) -> Subscription:
    return Subscription(
        id=sub_id,
        type="live",
        name=f"主播{uid}",
        uid=uid,
        poll_interval_sec=interval,
        enabled=enabled,
        push_session_ids=[_SESSION],
    )


def _collection_sub(sub_id: str, uid: int, interval: int = 300) -> Subscription:
    return Subscription(
        id=sub_id,
        type="collection",
        name="测试合集订阅",
        uid=uid,
        list_id=1,
        series_type=0,
        poll_interval_sec=interval,
        push_session_ids=[_SESSION],
    )


def _dynamic_sub(sub_id: str, uid: int, interval: int = 300) -> Subscription:
    return Subscription(
        id=sub_id,
        type="dynamic",
        name="测试动态订阅",
        uid=uid,
        poll_interval_sec=interval,
        push_session_ids=[_SESSION],
    )


def _make_scheduler(
    subs: list[Subscription],
    repo: Any,
    db: InstantDb,
    clock: FakeClock,
    sleep: Any,
    poll_settings: dict[str, Any] | None = None,
    context: FakeContext | None = None,
    status: dict[str, Any] | None = None,
    retry_counts: dict[str, dict[str, int]] | None = None,
) -> tuple[Scheduler, dict[str, Any]]:
    status = status if status is not None else {}
    scheduler = Scheduler(
        subscriptions=subs,
        credential_cfg={},
        repo=repo,
        db=db,
        build_chain=build_chain,
        send=send,
        context=context if context is not None else FakeContext(),
        status=status,
        retry_counts=retry_counts if retry_counts is not None else {},
        poll_settings=poll_settings if poll_settings is not None else {},
        now=clock,
        sleep=sleep,
    )
    return scheduler, status


async def _drive(condition: Callable[[], bool]) -> None:
    """让出事件循环直至条件满足（上限防死循环）。"""
    for _ in range(_MAX_ITERS):
        if condition():
            return
        await asyncio.sleep(0)
    raise AssertionError("drive timeout: 条件在迭代上限内未满足")


async def _settle(rounds: int = 20) -> None:
    """让出几次事件循环，保证任务注册/唤醒有确定性机会。"""
    for _ in range(rounds):
        await asyncio.sleep(0)


# ----------------------------------------------------------------------
# 1/2. 令牌桶
# ----------------------------------------------------------------------


def test_token_bucket_capacity_three_then_blocks() -> None:
    """容量 3、速率 0.5/s：3 次立即取牌，第 4 次阻塞 2s 后取到。"""

    async def scenario() -> None:
        clock = FakeClock()
        deltas: list[float] = []

        async def fake_sleep(sec: float) -> None:
            deltas.append(sec)
            clock.tick(sec)

        bucket = TokenBucket(3, 0.5, now=clock, sleep=fake_sleep)
        for _ in range(3):
            await bucket.acquire()
        assert deltas == []  # 前 3 次无需等待
        assert clock.t == _T0
        await bucket.acquire()  # 第 4 次：等 (1-0)/0.5 = 2s
        assert deltas == [2.0]
        assert clock.t == _T0 + 2

    asyncio.run(scenario())


def test_token_bucket_refill_accumulates_capped() -> None:
    """补充按流逝时间累积且以容量封顶：闲置 10s（应补 5 枚→封顶 3）。"""

    async def scenario() -> None:
        clock = FakeClock()
        deltas: list[float] = []

        async def fake_sleep(sec: float) -> None:
            deltas.append(sec)
            clock.tick(sec)

        bucket = TokenBucket(3, 0.5, now=clock, sleep=fake_sleep)
        clock.tick(10)  # 闲置：10s × 0.5 = 5 枚 → 封顶 3
        assert await bucket.try_acquire_nowait() is True
        assert await bucket.try_acquire_nowait() is True
        assert await bucket.try_acquire_nowait() is True
        assert await bucket.try_acquire_nowait() is False  # 第 4 枚非阻塞取不到
        await bucket.acquire()
        assert deltas == [2.0]  # 阻塞至补充 1 枚
        assert clock.t == _T0 + 10 + 2

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 3. 错误退避
# ----------------------------------------------------------------------


def test_backoff_sequence_grows_and_caps() -> None:
    """连续失败退避 2^n、上限 5min（300s）。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        repo.live_error = BiliNetworkError("网络炸了")
        scheduler, status = _make_scheduler(
            [_live_sub("live-1", 10086, interval=5)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _drive(lambda: len(sleep.non_idle()) >= 18)
        non_idle = sleep.non_idle()
        assert non_idle == [
            5,
            2,
            5,
            4,
            5,
            8,
            5,
            16,
            5,
            32,
            5,
            64,
            5,
            128,
            5,
            256,
            5,
            300,
        ]
        assert non_idle[0::2] == [5] * 9  # 间隔
        assert non_idle[1::2] == [2, 4, 8, 16, 32, 64, 128, 256, 300]  # 退避
        assert status["live-1"].error_count == 9
        await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 4. 自动禁用
# ----------------------------------------------------------------------


def test_auto_disable_after_10_failures_with_alert() -> None:
    """10 连败 → auto_disabled + 告警推送；第 11 轮整轮跳过。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        repo.live_error = BiliNetworkError("网络炸了")
        context = FakeContext()
        scheduler, status = _make_scheduler(
            [_live_sub("live-1", 10086, interval=5)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
            context=context,
        )
        scheduler.start()
        await _drive(lambda: getattr(status.get("live-1"), "auto_disabled", False))
        assert status["live-1"].auto_disabled is True
        assert status["live-1"].error_count == 10
        total_calls = sum(repo.live_info_calls.values())
        assert total_calls == 10  # 恰好 10 轮请求后禁用
        assert len(context.sent) == 1  # 告警推送到其全部会话
        session, chain = context.sent[0]
        assert session == _SESSION
        assert "自动禁用" in str(chain)
        # 第 11 轮起整轮跳过：不再有任何 B 站请求
        await _settle()
        assert sum(repo.live_info_calls.values()) == total_calls
        await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 5. 逐订阅间隔
# ----------------------------------------------------------------------


def test_per_sub_interval_respected() -> None:
    """两个不同间隔的 live 订阅 → 睡眠序列 [10,20,10,20,...]。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        sub_a = _live_sub("live-a", 10086, interval=10)
        sub_b = _live_sub("live-b", 10087, interval=20)
        scheduler, _ = _make_scheduler(
            [sub_a, sub_b],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 6)
        non_idle = sleep.non_idle()
        assert non_idle[:6] == [10, 20, 10, 20, 10, 20]
        # 每订阅首轮 seed 各解析一次 room_id，其后轮次走 room_id 缓存
        assert repo.live_info_calls == {10086: 1, 10087: 1}
        assert repo.room_info_calls >= 6  # 至少各 3 轮（驱动退出时机可能多一轮）
        await scheduler.stop()

    asyncio.run(scenario())


def test_same_type_subs_poll_independently() -> None:
    """3 个同类型订阅各自独立轮询：先各自睡满自身间隔再轮询，互不串行等待。

    旧实现的 per-type 串行循环会先 sleep→poll→sleep→poll；新实现为每个订阅
    一个任务，因此事件序列以 3 个连续 sleep 开头（每个订阅先睡满自己的间隔）。
    """

    async def scenario() -> None:
        clock = FakeClock()
        events: list[str] = []

        async def recording_sleep(sec: float) -> None:
            events.append("sleep")
            clock.tick(sec)
            await asyncio.sleep(0)

        repo = LiveFakeRepo()

        async def recording_room_info(room_id: int) -> dict:
            events.append("poll")
            return dict(_room(0))

        repo.get_room_info = recording_room_info  # type: ignore[method-assign]
        subs = [_live_sub(f"live-{i}", 10086 + i, interval=10) for i in range(3)]
        scheduler, _ = _make_scheduler(
            subs,
            repo,
            InstantDb(),
            clock,
            recording_sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _drive(lambda: len(events) >= 9)
        # 前 3 个事件都是 sleep：3 个订阅各自先睡满自身间隔，互不串行
        assert events[:3] == ["sleep", "sleep", "sleep"]
        # 之后每个订阅按 轮询→睡眠 节奏推进（poll 后紧跟自己的 sleep）
        assert events[3:9] == ["poll", "sleep", "poll", "sleep", "poll", "sleep"]
        await scheduler.stop()

    asyncio.run(scenario())


def test_bucket_rate_equals_aggregate_demand() -> None:
    """令牌桶速率 = max(聚合轮询需求, 1/global_min)，保证桶不拖慢配置间隔。"""

    subs = [_live_sub(f"live-{i}", 10086 + i, interval=120) for i in range(3)]
    scheduler = Scheduler(
        subscriptions=subs,
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
        poll_settings={"global_min_interval_sec": 60, "poll_jitter_sec": 0},
    )
    assert abs(scheduler._bucket_rate() - 3 / 120) < 1e-9  # 聚合需求高于下限

    single = Scheduler(
        subscriptions=[_live_sub("live-1", 10086, interval=300)],
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
        poll_settings={"global_min_interval_sec": 60, "poll_jitter_sec": 0},
    )
    assert abs(single._bucket_rate() - 1 / 60) < 1e-9  # 需求低于下限时取 1/global_min


# ----------------------------------------------------------------------
# 6. 重建 + clear_disabled
# ----------------------------------------------------------------------


def test_rebuild_preserves_status_and_clear_disabled() -> None:
    """重建：旧任务取消、新 poller 生效、auto-disable 保留；clear_disabled 清空。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        context = FakeContext()
        sub_a = _live_sub("live-a", 10086, interval=5)
        scheduler, status = _make_scheduler(
            [sub_a],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
            context=context,
        )
        retry_counts = scheduler.retry_counts
        # 阶段 1：A 持续失败 → 自然自动禁用
        repo.live_error = BiliNetworkError("网络炸了")
        scheduler.start()
        await _drive(lambda: getattr(status.get("live-a"), "auto_disabled", False))
        assert status["live-a"].auto_disabled is True
        old_tasks = list(scheduler._tasks)

        # 阶段 2：重建（不清禁用标志）→ 旧任务取消、B 生效、标志保留
        sub_b = _live_sub("live-b", 10087, interval=5)
        repo.live_error = None
        await scheduler.rebuild([sub_b], {}, clear_disabled=False)
        assert all(task.done() for task in old_tasks)
        assert set(scheduler.pollers) == {"live-b"}
        assert scheduler.status is status  # 同一 dict 对象
        assert scheduler.retry_counts is retry_counts
        assert status["live-a"].auto_disabled is True  # 重建保留
        await _drive(lambda: repo.live_info_calls.get(10087, 0) >= 1)
        assert repo.live_info_calls.get(10087, 0) >= 1  # B 正常轮询

        # 阶段 3：clear_disabled() 清空运行时禁用标志
        scheduler.clear_disabled()
        assert status["live-a"].auto_disabled is False

        # 阶段 4：重建时 clear_disabled=True 同样清空
        status["live-a"].auto_disabled = True
        await scheduler.rebuild([sub_b], {}, clear_disabled=True)
        assert status["live-a"].auto_disabled is False

        await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 7. 限速：桶空时请求阻塞直到时钟推进
# ----------------------------------------------------------------------


def test_per_poll_token_multi_page_not_blocked() -> None:
    """per-poll 限速：每轮只取一枚令牌，合集 2 页/轮不再在页间阻塞。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = ControlledSleep(clock)
        items = [
            {"bvid": f"BV{i:04d}", "pubdate": 1_700_000_000 + i} for i in range(25)
        ]
        repo = CollectionFakeRepo(items)
        scheduler, _ = _make_scheduler(
            [_collection_sub("col-1", 10086, interval=5)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 5, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _settle()  # 任务先注册首轮睡眠，再开始受控推进
        # 每轮推进 5s：一轮取一枚令牌，2 页请求全部立即完成（无页间阻塞）
        await sleep.advance(5)
        assert repo.calls == 2
        await sleep.advance(5)
        assert repo.calls == 4
        await sleep.advance(5)
        assert repo.calls == 6
        await scheduler.stop()

    asyncio.run(scenario())


def test_rate_limit_capacity_blocks_fourth_sub() -> None:
    """桶容量 3：4 个订阅同时轮询时第 4 个阻塞在令牌桶（时钟推进前不放行）。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = ControlledSleep(clock)
        repo = CollectionFakeRepo([{"bvid": "BV0001", "pubdate": 1_700_000_000}])
        subs = [_collection_sub(f"col-{i}", 10086 + i, interval=5) for i in range(4)]
        scheduler, _ = _make_scheduler(
            subs,
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 5, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _settle()  # 4 个任务先注册首轮睡眠
        await sleep.advance(5)
        await _settle()
        # 容量 3：仅 3 个订阅完成首轮轮询，第 4 个阻塞等待令牌
        assert repo.calls == 3
        # 无时钟推进 → 仍阻塞，请求数不增长
        await _settle()
        assert repo.calls == 3
        await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 8. stop 取消
# ----------------------------------------------------------------------


def test_stop_cancels_all_tasks_without_unhandled_cancelled() -> None:
    """stop() 干净取消全部任务；可再次 start 继续轮询。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        scheduler, _ = _make_scheduler(
            [_live_sub("live-1", 10086, interval=5)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 2)
        tasks = list(scheduler._tasks)
        await scheduler.stop()  # 不抛任何异常
        assert scheduler._tasks == []
        assert all(task.done() for task in tasks)
        await scheduler.stop()  # 幂等
        # 再次 start 可继续轮询
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 4)
        await scheduler.stop()

    asyncio.run(scenario())


# ----------------------------------------------------------------------
# 9. 维护任务
# ----------------------------------------------------------------------


def test_maintenance_prune_called_every_6h(monkeypatch) -> None:
    """维护任务每 6h 调一次 db.prune_old。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = ControlledSleep(clock)
        db = InstantDb()
        scheduler, _ = _make_scheduler([], LiveFakeRepo(), db, clock, sleep)
        calls: dict[str, int] = {"n": 0}

        async def counting_prune() -> None:
            calls["n"] += 1

        monkeypatch.setattr(db, "prune_old", counting_prune)
        task = scheduler.create_maintenance_task()
        assert scheduler.create_maintenance_task() is task  # 稳定引用
        await _settle()  # 让维护任务先注册首轮 6h 睡眠
        # 受控推进：每过 6h 恰调用一次 prune_old
        await sleep.advance(6 * 3600)
        assert calls["n"] == 1
        await sleep.advance(6 * 3600)
        assert calls["n"] == 2
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

    asyncio.run(scenario())


def test_poll_one_exception_does_not_kill_task() -> None:
    """_poll_one 账务异常（非 CancelledError）不会杀死订阅轮询任务。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        db = InstantDb()
        scheduler, _ = _make_scheduler(
            [_live_sub("live-1", 10086, interval=1)],
            repo,
            db,
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        real_poll_one = scheduler._poll_one
        calls: dict[str, int] = {"n": 0}

        async def flaky(sub, poller) -> None:
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("账务爆炸")
            await real_poll_one(sub, poller)

        scheduler._poll_one = flaky  # type: ignore[method-assign]
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 2)
        assert repo.room_info_calls >= 2  # 任务存活并继续轮询
        await scheduler.stop()

    asyncio.run(scenario())


def test_live_poller_receives_epoch_clock() -> None:
    """直播轮询器使用 epoch 时钟：monotonic 无纪元会把下播时间算成 1970 年。"""

    scheduler = Scheduler(
        subscriptions=[_live_sub("live-1", 10086, interval=60)],
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
    )
    # 生产默认：epoch 时钟给出 2023+ 年的秒级时间戳（远大于 monotonic 开机时长）
    assert scheduler._epoch_now() > 1_000_000_000
    pollers = scheduler._build_pollers()
    assert pollers["live-1"].now is scheduler._epoch_now


def test_push_cover_string_false_respected() -> None:
    """手改配置把 push_dynamic_cover 写成字符串 "false" → 正确关闭（不再误判开）。"""

    scheduler = Scheduler(
        subscriptions=[_live_sub("live-1", 10086, interval=60)],
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
        poll_settings={
            "push_dynamic_cover": "false",
            "push_live_cover": "false",
            "push_collection_cover": False,
            "push_dynamic_live_share": "true",
        },
    )
    assert scheduler._push_dynamic_cover is False
    assert scheduler._push_live_cover is False
    assert scheduler._push_collection_cover is False
    assert scheduler._push_dynamic_live_share is True
    assert "动态封面=关" in scheduler.push_settings_summary()
    assert "直播封面=关" in scheduler.push_settings_summary()


def test_push_cover_flag_reaches_dynamic_poller() -> None:
    """push_dynamic_cover=false 会真正传递到动态轮询器（端到端接线）。"""

    scheduler = Scheduler(
        subscriptions=[_dynamic_sub("dyn-1", 10086, interval=60)],
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
        poll_settings={"push_dynamic_cover": False},
    )
    pollers = scheduler._build_pollers()
    assert pollers["dyn-1"].push_cover is False


def test_poll_settings_non_finite_fall_back_to_defaults() -> None:
    """手改配置写入 inf/nan → 回退默认值（否则 sleep(inf) 让轮询任务永久挂起）。"""

    scheduler = Scheduler(
        subscriptions=[_live_sub("live-1", 10086, interval=60)],
        credential_cfg={},
        repo=object(),
        db=None,
        build_chain=build_chain,
        send=send,
        context=FakeContext(),
        status={},
        retry_counts={},
        poll_settings={
            "global_min_interval_sec": float("inf"),
            "poll_jitter_sec": float("nan"),
        },
    )
    assert scheduler._global_min == 60.0
    assert scheduler._jitter == 0.0
    scheduler._apply_poll_settings(
        {"global_min_interval_sec": float("-inf"), "poll_jitter_sec": float("inf")}
    )
    assert scheduler._global_min == 60.0
    assert scheduler._jitter == 0.0


def test_push_settings_change_logged_on_change_only() -> None:
    """推送开关变更时打印摘要；相同设置重复应用时不重复打印。"""

    class _Rec(logging.Handler):
        def __init__(self) -> None:
            super().__init__()
            self.records: list[logging.LogRecord] = []

        def emit(self, record: logging.LogRecord) -> None:
            self.records.append(record)

    logger = logging.getLogger("test_scheduler.settings_log")
    logger.handlers.clear()
    logger.setLevel(logging.INFO)
    handler = _Rec()
    logger.addHandler(handler)
    try:
        scheduler = Scheduler(
            subscriptions=[_live_sub("live-1", 10086, interval=60)],
            credential_cfg={},
            repo=object(),
            db=None,
            build_chain=build_chain,
            send=send,
            context=FakeContext(),
            status={},
            retry_counts={},
            logger=logger,
            poll_settings={"push_dynamic_cover": False},
        )
        summaries = [
            r.getMessage() for r in handler.records if "推送开关" in r.getMessage()
        ]
        assert len(summaries) == 1  # 初始化打印一次
        assert "动态封面=关" in summaries[0]

        scheduler._apply_poll_settings({"push_dynamic_cover": False})
        assert (
            len([r for r in handler.records if "推送开关" in r.getMessage()]) == 1
        )  # 未变更不重复打印

        scheduler._apply_poll_settings({"push_dynamic_cover": True})
        summaries = [
            r.getMessage() for r in handler.records if "推送开关" in r.getMessage()
        ]
        assert len(summaries) == 2  # 变更后再次打印
        assert "动态封面=开" in summaries[-1]
    finally:
        logger.removeHandler(handler)


# ----------------------------------------------------------------------
# 10. 下播确认快速复查 + last_poll 观测后写入
# ----------------------------------------------------------------------


def test_offline_confirm_fast_recheck_shortens_delay() -> None:
    """下播确认中按短间隔（15s）快速复查：下播推送不再等满完整轮询间隔。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        context = FakeContext()
        scheduler, _ = _make_scheduler(
            [_live_sub("live-1", 10086, interval=10)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
            context=context,
        )
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 1)  # seed 完成
        repo.room = _room(1)
        await _drive(lambda: any("【B站开播】" in str(c[1]) for c in context.sent))
        repo.room = _room(0)
        await _drive(lambda: any("【B站下播】" in str(c[1]) for c in context.sent))
        non_idle = sleep.non_idle()
        assert 15.0 in non_idle  # 下播确认中的快速复查间隔出现
        assert non_idle.count(10.0) >= 2  # seed/开播/常规轮次按完整间隔推进
        assert len(context.sent) >= 2  # 开播 + 下播
        await scheduler.stop()

    asyncio.run(scenario())


def test_last_poll_recorded_after_observation() -> None:
    """status.last_poll 在观测完成之后写入（贴近真实轮询点，非轮询开始前）。"""

    async def scenario() -> None:
        clock = FakeClock()
        sleep = AutoSleep(clock)
        repo = LiveFakeRepo()
        wall: dict[str, str] = {"iso": "before"}

        async def recording_room_info(room_id: int) -> dict:
            del room_id
            repo.room_info_calls += 1
            wall["iso"] = "polled"  # 观测执行中打标记
            return dict(_room(0))

        repo.get_room_info = recording_room_info  # type: ignore[method-assign]
        scheduler, status = _make_scheduler(
            [_live_sub("live-1", 10086, interval=10)],
            repo,
            InstantDb(),
            clock,
            sleep,
            poll_settings={"global_min_interval_sec": 1, "poll_jitter_sec": 0},
        )
        scheduler._now_iso = lambda: wall["iso"]
        scheduler.start()
        await _drive(lambda: repo.room_info_calls >= 2)
        await _settle()
        assert status["live-1"].last_poll == "polled"  # 观测完成后写入
        await scheduler.stop()

    asyncio.run(scenario())


def test_live_awaiting_confirm_flag() -> None:
    """_live_awaiting_confirm：观测到离线（计数≥1）且未通知 → True；
    已通知/仍在直播/从未直播 → False。"""

    def state(**fields: Any) -> Any:
        base = SimpleNamespace(
            last_status=None,
            consecutive_offline_count=None,
            offline_notified=None,
        )
        for key, value in fields.items():
            setattr(base, key, value)
        return base

    assert Scheduler._live_awaiting_confirm(None) is False
    assert (
        Scheduler._live_awaiting_confirm(
            state(last_status=0, consecutive_offline_count=1, offline_notified=0)
        )
        is True
    )
    assert (
        Scheduler._live_awaiting_confirm(
            state(last_status=0, consecutive_offline_count=1, offline_notified=1)
        )
        is False  # 已通知
    )
    assert (
        Scheduler._live_awaiting_confirm(
            state(last_status=1, consecutive_offline_count=0, offline_notified=0)
        )
        is False  # 仍在直播
    )
    assert (
        Scheduler._live_awaiting_confirm(
            state(last_status=0, consecutive_offline_count=0, offline_notified=0)
        )
        is False  # 从未直播
    )
