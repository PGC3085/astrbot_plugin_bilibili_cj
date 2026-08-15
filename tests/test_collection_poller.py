"""合集轮询器单元测试（计划 todo 8）。

全部离线：fake repository（脚本化分页）、临时 SQLite（``tmp_path``）、
记录式 fake context、真实 ``push.build_chain``/``push.send``（离线 str 模式）。
每个用例在单个 ``asyncio.run`` 内完成 init → 轮询 → close（aiosqlite 连接
不跨 event loop 复用）。

覆盖计划验收点：

1. seed 后无新增不推送；2. seed 后新增只推一次（含载荷字段）；3.
``series_type`` 0/1 正确透传仓库；4. 全量翻页到结尾（无页数上限）；5.
**不遇到首个已见即停**（page 1 全是旧项、page 2 有新项仍推送，及 page 内
中间插入）；6. 同一 bvid 两轮只推一次；7. 全失败 mark-after-send 重试，
3 轮后仍标记 + 告警；8. seed 持久化：重建 poller 不重 seed，停机期新视频
照常推送；9. 空合集不推送不报错；10. 仓库 ``BiliNetworkError`` 吞掉记日志
不崩溃（另有未知异常吞掉、``CancelledError`` 透传两个补充用例）。
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from config import Subscription
from db import Database
from poller.collection import CollectionPoller
from push import build_chain, send
from repository import BiliNetworkError

_SESSION = "aiocqhttp:GroupMessage:123"


class FakeRepo:
    """脚本化 BiliRepository fake：按 ``ps`` 切片服务一个扁平 item 列表。

    ``calls`` 记录每次 ``(uid, list_id, series_type, pn, ps)``；``error``
    设置后在下一次 ``get_videos`` 抛出一次即清空。
    """

    def __init__(self, items: list[dict]) -> None:
        self.items = list(items)
        self.calls: list[tuple[int, int, int, int, int]] = []
        self.error: Exception | None = None

    async def get_videos(
        self, uid: int, list_id: int, series_type: int, pn: int, ps: int
    ) -> dict:
        self.calls.append((uid, list_id, series_type, pn, ps))
        if self.error is not None:
            exc, self.error = self.error, None
            raise exc
        start = (pn - 1) * ps
        return {
            "archives": self.items[start : start + ps],
            "meta": {"name": "测试合集", "id": list_id},
        }


class FakeContext:
    """记录 ``send_message`` 调用；默认全部投递成功。"""

    def __init__(self, ok: bool = True) -> None:
        self.ok = ok
        self.sent: list[tuple[str, object]] = []

    async def send_message(self, session: str, chain: object) -> bool:
        self.sent.append((session, chain))
        return self.ok


class _RecordListHandler(logging.Handler):
    """收集 LogRecord 的 handler，供断言日志内容。"""

    def __init__(self) -> None:
        super().__init__()
        self.records: list[logging.LogRecord] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.records.append(record)


def _recording_logger() -> tuple[logging.Logger, list[logging.LogRecord]]:
    logger = logging.getLogger(f"test_collection_poller.{uuid4().hex}")
    logger.propagate = False
    handler = _RecordListHandler()
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger, handler.records


def _item(i: int, title: str | None = None) -> dict:
    return {
        "bvid": f"BV{i:04d}",
        "aid": i,
        "pubdate": 1_700_000_000 + i,
        "title": title or f"视频{i}",
        "pic": f"https://example.com/pic{i}.jpg",
    }


def _make_subscription(
    sub_id: str = "sub-1", series_type: int = 0, list_id: int = 1
) -> Subscription:
    return Subscription(
        id=sub_id,
        type="collection",
        name="测试合集订阅",
        uid=10086,
        list_id=list_id,
        series_type=series_type,
        push_session_ids=[_SESSION],
    )


def _make_poller(
    repo: FakeRepo,
    db: Database,
    context: FakeContext | None = None,
    retry_counts: dict | None = None,
    logger: logging.Logger | None = None,
    subscription: Subscription | None = None,
    push_cover: bool = True,
) -> tuple[CollectionPoller, dict]:
    status: dict = {
        subscription.id if subscription else "sub-1": SimpleNamespace(
            last_push_at=None, last_error=None
        )
    }
    poller = CollectionPoller(
        subscription=subscription or _make_subscription(),
        repo=repo,
        db=db,
        build_chain=build_chain,
        send=send,
        context=context or FakeContext(),
        status=status,
        retry_counts=retry_counts if retry_counts is not None else {},
        logger=logger,
        push_cover=push_cover,
    )
    return poller, status


def _expected_pub(item: dict) -> str:
    return datetime.fromtimestamp(
        item["pubdate"], tz=timezone(timedelta(hours=8))
    ).strftime("%Y-%m-%d %H:%M:%S")


def test_seed_then_no_new_no_push(tmp_path) -> None:
    """1. seed（25 条，2 页 20/5）静默；第二轮无新增不推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(25)])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()
            assert context.sent == []  # seed 静默
            assert await db.get_seeded("collection_state_v2", "sub-1")
            await poller.poll()
            assert context.sent == []
            assert [c[3] for c in repo.calls] == [1, 2, 1, 2]  # 每轮 2 页
        finally:
            await db.close()

    asyncio.run(scenario())


def test_new_bvid_after_seed_pushed_once(tmp_path) -> None:
    """2. seed 后追加 1 新 → 只推一次，且载荷字段齐全。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(25)])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed
            repo.items.append(_item(25, title="新视频"))
            await poller.poll()
            assert len(context.sent) == 1
            session, chain = context.sent[0]
            assert session == _SESSION
            assert isinstance(chain, str)
            assert "新视频" in chain
            assert "合集：测试合集" in chain
            assert f"发布时间：{_expected_pub(_item(25))}" in chain
            assert "https://www.bilibili.com/video/BV0025" in chain
            await poller.poll()  # 再轮：不重复推送
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


@pytest.mark.parametrize("series_type", [0, 1])
def test_series_type_passed_to_repo(tmp_path, series_type: int) -> None:
    """3. series_type 0/1 分别透传 repo.get_videos。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(5)])
        poller, _ = _make_poller(
            repo, db, subscription=_make_subscription(series_type=series_type)
        )
        try:
            await poller.poll()
            assert repo.calls, "get_videos 应被调用"
            for uid, list_id, passed_series_type, pn, ps in repo.calls:
                assert passed_series_type == series_type
                assert uid == 10086
                assert list_id == 1
                assert ps == 20
        finally:
            await db.close()

    asyncio.run(scenario())


def test_pagination_scans_all_pages_no_cap(tmp_path) -> None:
    """4. 45 条（3 页 20/20/5）全量扫到尾，无页数上限。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(45)])
        poller, _ = _make_poller(repo, db)
        try:
            await poller.poll()
            assert [c[3] for c in repo.calls] == [1, 2, 3]
        finally:
            await db.close()

    asyncio.run(scenario())


def test_does_not_stop_at_first_seen(tmp_path) -> None:
    """5a. page 1 全旧项、page 2 有新项 → page 2 新项仍推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(20)])  # 恰满 1 页
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 20
            repo.items.extend([_item(20), _item(21)])  # 新项落在 page 2
            await poller.poll()
            assert len(context.sent) == 2
            urls = [str(chain).split("链接：")[-1] for _, chain in context.sent]
            assert set(urls) == {
                "https://www.bilibili.com/video/BV0020",
                "https://www.bilibili.com/video/BV0021",
            }
        finally:
            await db.close()

    asyncio.run(scenario())


def test_new_item_mid_page_still_pushed(tmp_path) -> None:
    """5b. page 1 中间插入新项（首个即旧项）仍被推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(20)])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed 20
            repo.items.insert(10, _item(100, title="中间插入"))
            await poller.poll()
            assert len(context.sent) == 1
            assert "中间插入" in str(context.sent[0][1])
        finally:
            await db.close()

    asyncio.run(scenario())


def test_dedup_same_bvid_pushed_once(tmp_path) -> None:
    """6. 同一 bvid 连续两轮只推一次。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(3)])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()  # seed
            repo.items.append(_item(3))
            await poller.poll()  # 推 BV0003
            await poller.poll()  # 同样内容
            assert len(context.sent) == 1
        finally:
            await db.close()

    asyncio.run(scenario())


def test_all_fail_retry_then_mark_and_warn(tmp_path) -> None:
    """7. 全失败不确认：重试计数 1→2→3，3 轮后告警并停止（仍标记）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(2)])
        context = FakeContext(ok=False)
        retry_counts: dict = {}
        logger, records = _recording_logger()
        poller, _ = _make_poller(
            repo, db, context=context, retry_counts=retry_counts, logger=logger
        )
        try:
            await poller.poll()  # seed
            repo.items.append(_item(2))
            await poller.poll()
            assert len(context.sent) == 1
            assert retry_counts["sub-1"]["BV0002"] == 1
            await poller.poll()
            assert len(context.sent) == 2
            assert retry_counts["sub-1"]["BV0002"] == 2
            await poller.poll()  # 第 3 轮：达上限 → 告警 + 清除计数（仍标记）
            assert len(context.sent) == 3
            assert "BV0002" not in retry_counts["sub-1"]
            assert any("已标记为已见" in r.getMessage() for r in records)
            await poller.poll()  # 第 4 轮：不再尝试
            assert len(context.sent) == 3
        finally:
            await db.close()

    asyncio.run(scenario())


def test_seed_persisted_new_poller_does_not_reseed(tmp_path) -> None:
    """8. seed 持久化：重建 poller 不重 seed，停机期新视频照常推送。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(5)])
        retry_counts: dict = {}
        context1 = FakeContext()
        poller1, _ = _make_poller(repo, db, context=context1, retry_counts=retry_counts)
        try:
            await poller1.poll()  # seed 5，静默
            assert context1.sent == []
            repo.items.append(_item(5))  # "停机期"新增
            context2 = FakeContext()
            poller2, _ = _make_poller(
                repo, db, context=context2, retry_counts=retry_counts
            )
            await poller2.poll()
            assert len(context2.sent) == 1  # 未重 seed → 新视频被推送
            assert context2.sent[0][0] == _SESSION
        finally:
            await db.close()

    asyncio.run(scenario())


def test_empty_collection_no_push_no_error(tmp_path) -> None:
    """9. 空合集：seed 即置位，任何轮次不推送不报错。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([])
        context = FakeContext()
        poller, _ = _make_poller(repo, db, context=context)
        try:
            await poller.poll()
            await poller.poll()
            assert context.sent == []
            assert await db.get_seeded("collection_state_v2", "sub-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_repo_network_error_swallowed(tmp_path) -> None:
    """10. BiliNetworkError 被吞掉记日志；恢复后下一轮正常 seed。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(3)])
        repo.error = BiliNetworkError("网络炸了")
        context = FakeContext()
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()  # 不抛异常
            assert context.sent == []
            assert any("轮询失败" in r.getMessage() for r in records)
            await poller.poll()  # 恢复：正常 seed
            assert context.sent == []
            assert await db.get_seeded("collection_state_v2", "sub-1")
        finally:
            await db.close()

    asyncio.run(scenario())


def test_unexpected_repo_error_swallowed(tmp_path) -> None:
    """补充：非 Bili 异常同样吞掉记日志，不崩溃。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(3)])
        repo.error = RuntimeError("boom")
        context = FakeContext()
        logger, records = _recording_logger()
        poller, _ = _make_poller(repo, db, context=context, logger=logger)
        try:
            await poller.poll()
            assert context.sent == []
            assert any("轮询异常" in r.getMessage() for r in records)
        finally:
            await db.close()

    asyncio.run(scenario())


def test_cancelled_error_re_raised(tmp_path) -> None:
    """补充：CancelledError 透传（任务取消不被吞成"网络错误"）。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        repo = FakeRepo([_item(i) for i in range(3)])
        repo.error = asyncio.CancelledError()
        poller, _ = _make_poller(repo, db)
        try:
            with pytest.raises(asyncio.CancelledError):
                await poller.poll()
        finally:
            await db.close()

    asyncio.run(scenario())


def test_collection_payload_cover_gated_by_setting(tmp_path) -> None:
    """push_cover=False 时合集载荷不携带封面；缺省 True 时携带。"""

    async def scenario() -> None:
        db = Database(tmp_path / "state.db")
        await db.init()
        item = _item(1)
        sub = _make_subscription()

        poller_on, _ = _make_poller(FakeRepo([]), db)
        payload = poller_on._payload(sub, "测试合集", item)
        assert payload["cover"] == item["pic"]

        poller_off, _ = _make_poller(FakeRepo([]), db, push_cover=False)
        payload = poller_off._payload(sub, "测试合集", item)
        assert "cover" not in payload
        await db.close()

    asyncio.run(scenario())
