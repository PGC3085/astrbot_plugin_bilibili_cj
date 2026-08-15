"""WebUI 后端单元测试（计划 todo 11）。

用 ``aiohttp.test_utils.TestServer`` / ``TestClient`` 起临时端口（port 0 临时端口），
以 ``asyncio.run`` 逐用例驱动（环境未装 pytest-asyncio，不新增依赖——aiohttp
已在 requirements.txt）。

覆盖：鉴权（无/错/正确 token、空 token 全拒、静态路由免鉴权）、订阅整表读写
（id 保留、合法落盘 + 重建、**非法条目整表 400 不部分落盘**）、设置往返、
测试推送（合法/非法会话/发送失败）、日志环形缓冲、端口占用失败降级、
stop 幂等与端口释放、首启 token 自动生成、_reject_reason 与 normalize 判定一致。
"""

import asyncio
import logging
from types import SimpleNamespace

import aiohttp
from aiohttp.test_utils import TestClient, TestServer

from config import normalize
from push import build_chain
from webui.server import WebUIServer

#: 离线环境 server._get_logger() 回退的 logger 名。
_LOGGER_NAME = "astrbot_plugin_bilibili_cj"

_AUTH = {"Authorization": "Bearer tok"}


def _make_config(**overrides):
    """构造测试用配置 dict（含 schema 全部顶层组）。"""
    config = {
        "credential": {"sessdata": "", "bili_jct": ""},
        "poll": {
            "global_min_interval_sec": 60,
            "poll_jitter_sec": 15,
            "push_title_change": True,
        },
        "webui": {"enabled": True, "host": "127.0.0.1", "port": 8765, "token": "tok"},
        "subscriptions": [],
    }
    config.update(overrides)
    return config


def _make_server(config=None, **kwargs):
    """构造 WebUIServer（token 缺省取自 _make_config 的 webui.token）。"""
    return WebUIServer(
        config if config is not None else _make_config(),
        request_rebuild=lambda clear_disabled: "rebuilt",
        status_provider=dict,
        token="tok",
        **kwargs,
    )


async def _start_client(server):
    """在临时端口（port 0）上启动 TestClient。"""
    client = TestClient(TestServer(server.app))
    await client.start_server()
    return client


class _RecordHandler(logging.Handler):
    """收集 LogRecord 的测试 handler。"""

    def __init__(self, records):
        super().__init__()
        self.records = records

    def emit(self, record):
        self.records.append(record)


def _run(coro):
    return asyncio.run(coro)


# ----------------------------------------------------------------------
# 1. 鉴权
# ----------------------------------------------------------------------


def test_auth_rejects_missing_and_wrong_token():
    """无 token / 错 token → 401；正确 token → 200；静态路由免鉴权。"""

    async def _case():
        server = _make_server()
        client = await _start_client(server)
        try:
            # 无 token
            resp = await client.get("/api/status")
            assert resp.status == 401
            assert (await resp.json()) == {"error": "unauthorized"}
            # 错 token
            resp = await client.get(
                "/api/status", headers={"Authorization": "Bearer wrong"}
            )
            assert resp.status == 401
            # 正确 token
            resp = await client.get("/api/status", headers=_AUTH)
            assert resp.status == 200
            # 静态路由不要求 token：无 token 也能拿到页面/资源（200 而非 401）
            resp = await client.get("/")
            assert resp.status == 200
            resp = await client.get("/assets/app.js")
            assert resp.status == 200
            # 不存在的静态文件仍 404（静态路径本身不受鉴权约束）
            resp = await client.get("/assets/whatever.js")
            assert resp.status == 404
        finally:
            await client.close()

    _run(_case())


def test_auth_empty_token_rejects_all():
    """有效 token 为空时（未 start 自动生成）任何请求都 401。"""

    async def _case():
        config = _make_config()
        config["webui"]["token"] = ""
        server = WebUIServer(
            config,
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="",
        )
        client = await _start_client(server)
        try:
            resp = await client.get(
                "/api/status", headers={"Authorization": "Bearer anything"}
            )
            assert resp.status == 401
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 2. GET /api/subscriptions
# ----------------------------------------------------------------------


def test_get_subscriptions_preserves_ids():
    """GET 返回规范化订阅，id 原样保留。"""

    async def _case():
        subs = [
            {
                "id": "live-1",
                "type": "live",
                "name": "主播A",
                "uid": 1001,
                "enabled": True,
                "push_session_ids": ["aiocqhttp:GroupMessage:1"],
            },
            {
                "id": "dyn-1",
                "type": "dynamic",
                "name": "主播B",
                "uid": 1002,
                "enabled": False,
                "push_session_ids": ["telegram:GroupMessage:9"],
            },
        ]
        server = _make_server(_make_config(subscriptions=subs))
        client = await _start_client(server)
        try:
            resp = await client.get("/api/subscriptions", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert [s["id"] for s in body["subscriptions"]] == ["live-1", "dyn-1"]
            live = body["subscriptions"][0]
            assert live["type"] == "live" and live["uid"] == 1001
            assert live["push_session_ids"] == ["aiocqhttp:GroupMessage:1"]
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 3. POST /api/subscriptions 合法整表
# ----------------------------------------------------------------------


def test_post_subscriptions_valid_saves_and_rebuilds():
    """合法整表 → 200、落盘、request_rebuild(clear_disabled=True)。"""

    async def _case():
        config = _make_config()
        save_calls = []
        rebuild_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        def rebuild(clear_disabled):
            rebuild_calls.append(clear_disabled)
            return "rebuilt"

        server = WebUIServer(
            config,
            request_rebuild=rebuild,
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            payload = {
                "subscriptions": [
                    {
                        "type": "live",
                        "name": "新主播",
                        "uid": 2001,
                        "push_session_ids": ["aiocqhttp:GroupMessage:7"],
                    }
                ]
            }
            resp = await client.post("/api/subscriptions", json=payload, headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["count"] == 1
            assert body["rebuild"] == "rebuilt"
            # 落盘一次、重建一次（clear_disabled=True）
            assert len(save_calls) == 1
            assert rebuild_calls == [True]
            # 配置内存已更新，且 normalize 分配了稳定 id
            assert config["subscriptions"][0]["uid"] == 2001
            assert config["subscriptions"][0]["id"]
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 4. POST /api/subscriptions 整表 400 语义
# ----------------------------------------------------------------------


def test_post_subscriptions_rejected_entry_whole_table_400():
    """任一非法条目 → 400 + 被拒条目定位；save_config 不被调用（不部分落盘）。"""

    async def _case():
        original = [
            {
                "id": "old-1",
                "type": "live",
                "uid": 1,
                "push_session_ids": ["aiocqhttp:GroupMessage:1"],
            }
        ]
        config = _make_config(subscriptions=list(original))
        save_calls = []
        rebuild_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        def rebuild(clear_disabled):
            rebuild_calls.append(clear_disabled)
            return "rebuilt"

        server = WebUIServer(
            config,
            request_rebuild=rebuild,
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            payload = {
                "subscriptions": [
                    {
                        "type": "live",
                        "uid": 3001,
                        "push_session_ids": ["aiocqhttp:GroupMessage:5"],
                    },
                    # 缺 list_id/series_type 的 collection → 被拒
                    {
                        "type": "collection",
                        "uid": 3002,
                        "push_session_ids": ["aiocqhttp:GroupMessage:5"],
                    },
                ]
            }
            resp = await client.post("/api/subscriptions", json=payload, headers=_AUTH)
            assert resp.status == 400
            body = await resp.json()
            assert body["ok"] is False
            assert len(body["rejected"]) == 1
            assert body["rejected"][0]["index"] == 1
            assert "list_id" in body["rejected"][0]["reason"]
            assert len(body["errors"]) == 1
            # 整表语义：不部分落盘——save_config 未被调用、配置未变
            assert save_calls == []
            assert rebuild_calls == []
            assert config["subscriptions"] == original
        finally:
            await client.close()

    _run(_case())


def test_post_subscriptions_bad_body_400():
    """请求体非对象 / subscriptions 非数组 → 400，不落盘。"""

    async def _case():
        config = _make_config()
        save_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        server = WebUIServer(
            config,
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/subscriptions",
                json={"subscriptions": "not-a-list"},
                headers=_AUTH,
            )
            assert resp.status == 400
            resp = await client.post(
                "/api/subscriptions", data=b"not json", headers=_AUTH
            )
            assert resp.status == 400
            assert save_calls == []
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 5. GET/POST /api/settings
# ----------------------------------------------------------------------


def test_settings_roundtrip():
    """GET 返回三组设置；POST 写回 → 落盘 + 重建。"""

    async def _case():
        config = _make_config()
        save_calls = []
        rebuild_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        def rebuild(clear_disabled):
            rebuild_calls.append(clear_disabled)
            return "rebuilt"

        server = WebUIServer(
            config,
            request_rebuild=rebuild,
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.get("/api/settings", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert set(body) == {"credential", "poll", "webui"}
            assert body["poll"]["global_min_interval_sec"] == 60
            # 写回 poll + credential
            resp = await client.post(
                "/api/settings",
                json={
                    "poll": {"global_min_interval_sec": 120},
                    "credential": {"sessdata": "abc"},
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            assert (await resp.json())["ok"] is True
            assert config["poll"]["global_min_interval_sec"] == 120
            assert config["credential"]["sessdata"] == "abc"
            assert len(save_calls) == 1
            assert rebuild_calls == [True]
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 6. POST /api/test-push
# ----------------------------------------------------------------------


def test_test_push_valid_and_invalid_session():
    """合法会话 → 200 ok:true 且 send_to 收到 (session, chain)；非法会话 → 400。"""

    async def _case():
        sent = []

        async def send_to(session, chain):
            sent.append((session, chain))
            return True

        server = _make_server(send_to=send_to)
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={"session": "aiocqhttp:GroupMessage:123", "message": "你好"},
                headers=_AUTH,
            )
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert len(sent) == 1
            assert sent[0][0] == "aiocqhttp:GroupMessage:123"
            assert "你好" in str(sent[0][1])
            # 非法会话 → 400，不发送
            resp = await client.post(
                "/api/test-push",
                json={"session": "bad-session", "message": "x"},
                headers=_AUTH,
            )
            assert resp.status == 400
            assert (await resp.json())["ok"] is False
            assert len(sent) == 1
        finally:
            await client.close()

    _run(_case())


def test_test_push_send_failure_reports_ok_false():
    """send_to 返回 False / 抛异常 → 200 ok:false（不 500）。"""

    async def _case():
        async def send_to(session, chain):
            del session, chain
            return False

        server = _make_server(send_to=send_to)
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={"session": "aiocqhttp:GroupMessage:1", "message": "x"},
                headers=_AUTH,
            )
            assert resp.status == 200
            assert (await resp.json())["ok"] is False
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 7. GET /api/logs
# ----------------------------------------------------------------------


def test_logs_ring_buffer_tail():
    """feed_log 后 tail=N 返回 ≤N 条；tail=0 → 空；移除 handler 后停止收集。"""

    async def _case():
        server = _make_server()
        client = await _start_client(server)
        try:
            server.install_log_handler()
            logger = logging.getLogger(_LOGGER_NAME)
            logger.warning("marker-1")
            logger.warning("marker-2")
            logger.warning("marker-3")
            resp = await client.get("/api/logs?tail=2", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["total"] == 2 and len(body["logs"]) == 2
            assert body["logs"][-1].endswith("marker-3")
            # tail=0 → 0 条；无 tail → 默认 100（当前缓冲 3 条）
            resp = await client.get("/api/logs?tail=0", headers=_AUTH)
            assert (await resp.json())["logs"] == []
            resp = await client.get("/api/logs", headers=_AUTH)
            assert (await resp.json())["total"] == 3
            # 移除 handler 后不再收集
            server.remove_log_handler()
            logger.warning("marker-4")
            resp = await client.get("/api/logs?tail=10", headers=_AUTH)
            assert all("marker-4" not in line for line in (await resp.json())["logs"])
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 8. 端口占用失败降级
# ----------------------------------------------------------------------


def test_start_port_bind_failure_disables_server():
    """端口被占 → start 不抛、enabled=False、错误被记录、端口最终可重绑。"""

    async def _case():
        occupied = await asyncio.start_server(lambda r, w: None, "127.0.0.1", 0)
        port = occupied.sockets[0].getsockname()[1]
        records = []
        handler = _RecordHandler(records)
        logger = logging.getLogger(_LOGGER_NAME)
        logger.addHandler(handler)
        server = None
        try:
            server = _make_server()
            await server.start("127.0.0.1", port)  # 不应抛异常
            assert server.enabled is False
            assert any(r.levelno >= logging.ERROR for r in records), records
        finally:
            logger.removeHandler(handler)
            occupied.close()
            await occupied.wait_closed()
            if server is not None:
                await server.stop()
        # runner 已 cleanup：端口立即可以重绑
        rebind = await asyncio.start_server(lambda r, w: None, "127.0.0.1", port)
        rebind.close()
        await rebind.wait_closed()

    _run(_case())


# ----------------------------------------------------------------------
# 9. stop 幂等 + 端口释放
# ----------------------------------------------------------------------


def test_stop_idempotent_releases_port():
    """start 后可访问；stop 连停两次不抛，端口释放可立即重绑。"""

    async def _case():
        server = _make_server()
        await server.start("127.0.0.1", 0)
        assert server.enabled is True
        # 服务可访问（返回真实 index.html，证明请求确实到达服务而非连接失败）
        port = server._site._server.sockets[0].getsockname()[1]
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://127.0.0.1:{port}/") as resp:
                assert resp.status == 200
        # stop 幂等：连停两次
        await server.stop()
        await server.stop()
        assert server.enabled is False
        # 端口已释放，可立即重绑
        rebind = await asyncio.start_server(lambda r, w: None, "127.0.0.1", port)
        rebind.close()
        await rebind.wait_closed()

    _run(_case())


# ----------------------------------------------------------------------
# 10. 首启 token 自动生成
# ----------------------------------------------------------------------


def test_token_auto_generation_persists_and_logs_once():
    """空 token → start() 生成 uuid4 hex、持久化到配置、logger.info 一次。"""

    async def _case():
        config = _make_config()
        config["webui"]["token"] = ""
        save_calls = []
        records = []

        async def save(_cfg):
            save_calls.append(_cfg)

        handler = _RecordHandler(records)
        logger = logging.getLogger(_LOGGER_NAME)
        logger.setLevel(logging.INFO)  # 默认有效级别 WARNING 会滤掉 info 记录
        logger.addHandler(handler)
        server = None
        try:
            server = WebUIServer(
                config,
                request_rebuild=lambda clear_disabled: "rebuilt",
                status_provider=dict,
                token="",
                save_config=save,
            )
            assert server._token == ""
            await server.start("127.0.0.1", 0)
            token = server._token
            assert len(token) == 32  # uuid4 hex
            assert config["webui"]["token"] == token  # 已持久化
            assert save_calls == [config]
            infos = [r.getMessage() for r in records if r.levelno == logging.INFO]
            assert len(infos) == 1 and token in infos[0]
            # 生成的 token 可访问 API
            port = server._site._server.sockets[0].getsockname()[1]
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    f"http://127.0.0.1:{port}/api/status",
                    headers={"Authorization": f"Bearer {token}"},
                ) as resp:
                    assert resp.status == 200
            await server.stop()
        finally:
            logger.removeHandler(handler)
            if server is not None:
                await server.stop()

    _run(_case())


# ----------------------------------------------------------------------
# 附加：status 序列化 + 镜像一致性
# ----------------------------------------------------------------------


def test_status_serializes_namespace_entries():
    """status dict 的 SimpleNamespace 条目按字段序列化为 JSON。"""

    async def _case():
        status = {
            "sub-1": SimpleNamespace(
                last_poll="2026-01-01T00:00:00+00:00",
                last_error=None,
                error_count=0,
                live_status=1,
                last_push_at=None,
                auto_disabled=False,
            )
        }
        server = WebUIServer(
            _make_config(),
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=lambda: status,
            token="tok",
        )
        client = await _start_client(server)
        try:
            resp = await client.get("/api/status", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["sub-1"]["live_status"] == 1
            assert body["sub-1"]["error_count"] == 0
            assert body["sub-1"]["last_error"] is None
        finally:
            await client.close()

    _run(_case())


def test_reject_reason_matches_normalize():
    """锁定 _reject_reason 与 config.normalize 的接受/拒绝判定完全一致。"""
    cases = [
        {"type": "live", "uid": 1, "push_session_ids": ["aiocqhttp:GroupMessage:1"]},
        {
            "type": "collection",
            "uid": 2,
            "list_id": 10,
            "series_type": 0,
            "push_session_ids": ["aiocqhttp:GroupMessage:1"],
        },
        {"type": "bogus", "uid": 1, "push_session_ids": ["aiocqhttp:GroupMessage:1"]},
        {"type": "live", "push_session_ids": ["aiocqhttp:GroupMessage:1"]},  # 缺 uid
        {"type": "live", "uid": 1, "push_session_ids": []},  # 空会话
        {"type": "live", "uid": 1, "push_session_ids": ["bad"]},  # 全非法会话
        {"type": "live", "uid": 1, "push_session_ids": [123]},  # 非字符串会话
        {"type": "live", "uid": "x", "push_session_ids": ["aiocqhttp:GroupMessage:1"]},
        {
            "type": "collection",
            "uid": 1,
            "list_id": "x",
            "series_type": 0,
            "push_session_ids": ["aiocqhttp:GroupMessage:1"],
        },
        {
            "type": "live",
            "uid": 1,
            "push_session_ids": ["aiocqhttp:GroupMessage:1"],
            "poll_interval_sec": "abc",
        },
        "not-a-dict",
        None,
    ]
    server = _make_server()
    for raw in cases:
        normalized = normalize({"subscriptions": [raw], "poll": {}})
        accepted = len(normalized) == 1
        rejected = server._reject_reason(raw) is not None
        assert accepted != rejected, f"判定不一致: {raw!r}"


# ----------------------------------------------------------------------
# 11. POST /api/subscriptions/item（单条 upsert）
# ----------------------------------------------------------------------


def test_post_subscriptions_item_upsert_new():
    """无 id 的新条目 → 追加、落盘、重建，返回含分配 id 的完整列表。"""

    async def _case():
        config = _make_config()
        save_calls = []
        rebuild_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        def rebuild(clear_disabled):
            rebuild_calls.append(clear_disabled)
            return "rebuilt"

        server = WebUIServer(
            config,
            request_rebuild=rebuild,
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/subscriptions/item",
                json={
                    "subscription": {
                        "type": "live",
                        "name": "新主播",
                        "uid": 1,
                        "push_session_ids": ["aiocqhttp:GroupMessage:1"],
                    }
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["rebuild"] == "rebuilt"
            assert len(body["subscriptions"]) == 1
            assert body["subscriptions"][0]["uid"] == 1
            assert body["subscriptions"][0]["id"]
            assert len(save_calls) == 1
            assert rebuild_calls == [True]
            assert config["subscriptions"][0]["uid"] == 1
        finally:
            await client.close()

    _run(_case())


def test_post_subscriptions_item_replace_existing():
    """带既有 id 的条目 → 替换原条目，保持列表长度与 id 不变。"""

    async def _case():
        config = _make_config(
            subscriptions=[
                {
                    "id": "live-1",
                    "type": "live",
                    "name": "旧名",
                    "uid": 1,
                    "push_session_ids": ["aiocqhttp:GroupMessage:1"],
                }
            ]
        )
        save_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        server = WebUIServer(
            config,
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/subscriptions/item",
                json={
                    "subscription": {
                        "id": "live-1",
                        "type": "live",
                        "name": "新名",
                        "uid": 2,
                        "push_session_ids": ["aiocqhttp:GroupMessage:2"],
                    }
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert len(body["subscriptions"]) == 1
            assert body["subscriptions"][0]["id"] == "live-1"
            assert body["subscriptions"][0]["uid"] == 2
            assert body["subscriptions"][0]["name"] == "新名"
            assert len(save_calls) == 1
        finally:
            await client.close()

    _run(_case())


def test_post_subscriptions_item_invalid_400():
    """非法条目 → 400，不落盘。"""

    async def _case():
        config = _make_config()
        save_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        server = WebUIServer(
            config,
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/subscriptions/item",
                json={"subscription": {"type": "live", "push_session_ids": ["x"]}},
                headers=_AUTH,
            )
            assert resp.status == 400
            assert save_calls == []
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 12. DELETE /api/subscriptions/{sub_id}
# ----------------------------------------------------------------------


def test_delete_subscription_removes_and_rebuilds():
    """删除既有订阅 → 200、落盘、重建，返回剩余列表。"""

    async def _case():
        config = _make_config(
            subscriptions=[
                {
                    "id": "a",
                    "type": "live",
                    "uid": 1,
                    "push_session_ids": ["aiocqhttp:GroupMessage:1"],
                },
                {
                    "id": "b",
                    "type": "dynamic",
                    "uid": 2,
                    "push_session_ids": ["aiocqhttp:GroupMessage:1"],
                },
            ]
        )
        save_calls = []
        rebuild_calls = []

        async def save(_cfg):
            save_calls.append(_cfg)

        def rebuild(clear_disabled):
            rebuild_calls.append(clear_disabled)
            return "rebuilt"

        server = WebUIServer(
            config,
            request_rebuild=rebuild,
            status_provider=dict,
            token="tok",
            save_config=save,
        )
        client = await _start_client(server)
        try:
            resp = await client.delete("/api/subscriptions/a", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert len(body["subscriptions"]) == 1
            assert body["subscriptions"][0]["id"] == "b"
            assert len(save_calls) == 1
            assert rebuild_calls == [True]
        finally:
            await client.close()

    _run(_case())


def test_delete_subscription_missing_404():
    """删除不存在的订阅 → 404。"""

    async def _case():
        server = _make_server()
        client = await _start_client(server)
        try:
            resp = await client.delete("/api/subscriptions/nope", headers=_AUTH)
            assert resp.status == 404
            assert (await resp.json())["ok"] is False
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 13. POST /api/test-push 批量
# ----------------------------------------------------------------------


def test_test_push_batch_sessions():
    """sessions 数组 → 逐会话发送并返回 results；部分失败时 ok:false。"""

    async def _case():
        sent = []

        async def send_to(session, chain):
            sent.append((session, chain))
            return session.endswith(":ok")

        server = _make_server(send_to=send_to)
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={
                    "sessions": [
                        "aiocqhttp:GroupMessage:ok",
                        "aiocqhttp:GroupMessage:fail",
                    ],
                    "message": "批量试推",
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is False
            assert body["results"] == {
                "aiocqhttp:GroupMessage:ok": True,
                "aiocqhttp:GroupMessage:fail": False,
            }
            assert "成功 1/2" in body["detail"]
            assert len(sent) == 2
            assert all("批量试推" in str(chain) for _, chain in sent)
        finally:
            await client.close()

    _run(_case())


def test_test_push_batch_invalid_session_400():
    """批量中任一非法会话 → 400，不发送。"""

    async def _case():
        sent = []

        async def send_to(session, chain):
            sent.append((session, chain))
            return True

        server = _make_server(send_to=send_to)
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={"sessions": ["aiocqhttp:GroupMessage:1", "bad"], "message": "x"},
                headers=_AUTH,
            )
            assert resp.status == 400
            assert sent == []
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 14. 试推仿照对应推送格式（event_type）
# ----------------------------------------------------------------------


def test_test_push_event_type_live_on_uses_real_template():
    """event_type=live_on → 用真实开播模板渲染，含标题与开播时间。"""

    async def _case():
        sent = []

        async def send_to(session, chain):
            sent.append((session, chain))
            return True

        server = WebUIServer(
            _make_config(),
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            build_chain=build_chain,
            send_to=send_to,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={
                    "session": "aiocqhttp:GroupMessage:1",
                    "message": "测试标题",
                    "event_type": "live_on",
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            assert (await resp.json())["ok"] is True
            text = str(sent[0][1])
            assert "【B站开播】" in text
            assert "测试标题" in text
            assert "开播时间：" in text
        finally:
            await client.close()

    _run(_case())


def test_test_push_event_type_collection_uses_real_template():
    """event_type=collection → 用真实合集模板渲染，含视频标题与发布时间。"""

    async def _case():
        sent = []

        async def send_to(session, chain):
            sent.append((session, chain))
            return True

        server = WebUIServer(
            _make_config(),
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            build_chain=build_chain,
            send_to=send_to,
        )
        client = await _start_client(server)
        try:
            resp = await client.post(
                "/api/test-push",
                json={
                    "session": "aiocqhttp:GroupMessage:1",
                    "message": "测试视频",
                    "event_type": "collection",
                },
                headers=_AUTH,
            )
            assert resp.status == 200
            text = str(sent[0][1])
            assert "【B站合集更新】" in text
            assert "测试视频" in text
            assert "发布时间：" in text
        finally:
            await client.close()

    _run(_case())


def test_test_payload_maps_message_per_event_type():
    """_test_payload 按事件类型把 message 映射到最相关字段。"""
    server = _make_server()
    live_on = server._test_payload("live_on", "标题")
    assert live_on["title"] == "标题"
    assert "live_start_time" in live_on
    dynamic = server._test_payload("dynamic", "内容")
    assert dynamic["content"] == "内容"
    collection = server._test_payload("collection", "视频")
    assert collection["video_title"] == "视频"
    unknown = server._test_payload("bogus", "x")
    assert unknown["content"] == "x"


# ----------------------------------------------------------------------
# 15. GET /api/config-status
# ----------------------------------------------------------------------


def test_config_status_endpoint_returns_provider():
    """注入 provider 时原样返回其健康状态。"""

    async def _case():
        server = WebUIServer(
            _make_config(),
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            config_status_provider=lambda: {
                "path": "/x.json",
                "ok": False,
                "last_error": "boom",
            },
        )
        client = await _start_client(server)
        try:
            resp = await client.get("/api/config-status", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is False
            assert body["last_error"] == "boom"
            assert body["path"] == "/x.json"
        finally:
            await client.close()

    _run(_case())


def test_config_status_endpoint_defaults_ok():
    """未注入 provider 时返回默认正常状态。"""

    async def _case():
        server = _make_server()
        client = await _start_client(server)
        try:
            resp = await client.get("/api/config-status", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["ok"] is True
            assert body["last_error"] is None
        finally:
            await client.close()

    _run(_case())


# ----------------------------------------------------------------------
# 16. GET /api/login-status
# ----------------------------------------------------------------------


def test_login_status_endpoint_returns_provider():
    """注入 provider 时原样返回登录校验状态。"""

    async def _case():
        server = WebUIServer(
            _make_config(),
            request_rebuild=lambda clear_disabled: "rebuilt",
            status_provider=dict,
            token="tok",
            login_status_provider=lambda: {
                "last_ok_at": "2026-08-14T12:00:00+00:00",
                "consecutive_failures": 1,
                "last_error": "expired",
            },
        )
        client = await _start_client(server)
        try:
            resp = await client.get("/api/login-status", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["last_ok_at"] == "2026-08-14T12:00:00+00:00"
            assert body["consecutive_failures"] == 1
            assert body["last_error"] == "expired"
        finally:
            await client.close()

    _run(_case())


def test_login_status_endpoint_defaults():
    """未注入 provider 时返回默认登录状态。"""

    async def _case():
        server = _make_server()
        client = await _start_client(server)
        try:
            resp = await client.get("/api/login-status", headers=_AUTH)
            assert resp.status == 200
            body = await resp.json()
            assert body["last_ok_at"] is None
            assert body["consecutive_failures"] == 0
            assert body["last_error"] is None
        finally:
            await client.close()

    _run(_case())
