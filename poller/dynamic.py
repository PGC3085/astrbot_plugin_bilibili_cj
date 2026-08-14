"""动态轮询器：静默 seed + 空 offset 扫描推送新动态（计划 todo 7）。

- 每轮从空 offset（最新在前）拉取，翻页直到 ``has_more`` 为假或触
  :data:`_MAX_PAGES` 页上限（防失控；触上限记告警）。
- 首轮静默 seed：全部 ``dynamic_id`` 写入 ``known_dynamics``（不推送），
  seed 标志持久化到 ``dynamic_state.seeded``——重启恢复，避免重启后重 seed
  吞掉停机期新动态；即使首轮触页上限也照常置位。
- seed 后每轮只推送新项，按 ``(sub_id, dynamic_id)`` 经
  ``insert_dynamic_if_new`` 去重（PK 幂等吸收重复）。
- mark-after-send：任一 session 成功即视为已见；全部失败由
  ``retry_counts[sub_id][dynamic_id]``（main.py 持有、跨重建保留）计数重试，
  达 :data:`_MAX_RETRY_ROUNDS` 轮上限后仍标记并告警（行保留即"仍标记"）。
- :data:`DYNAMIC_TYPE_HANDLERS` 按类型码给出文案与内容格式化函数：
  8=视频投稿、2=图片、4=文字、1=转发、64=专栏、256=音频、2048=图文、
  4200/4308=直播分享、4300=收藏夹、4302=课程，其余走通用"发布新动态"。
  字段提取对新 API（``modules.module_dynamic.desc`` / ``major.*``）与旧 API
  （``desc`` / ``card`` JSON）均防御，缺失键返回空串。
- 仓库/未知异常吞掉记日志；``asyncio.CancelledError`` 透传。

``build_chain`` / ``send`` 由 main.py 注入 push 模块实现（离线测试可替换）。
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

try:
    from ..config import Subscription
    from ..db import Database
    from ..push import format_event_time
    from ..repository import BiliError, BiliRepository
except ImportError:  # pragma: no cover - 离线裸模块导入（自检脚本）
    from config import Subscription  # type: ignore[import-not-found]
    from db import Database  # type: ignore[import-not-found]
    from push import format_event_time  # type: ignore[import-not-found]
    from repository import BiliError, BiliRepository  # type: ignore[import-not-found]

#: 单轮扫描的页数上限（超出即停止翻页并告警）。
_MAX_PAGES: int = 10
#: 推送全失败后的最大重试轮数，达上限后仍标记为已见并告警。
_MAX_RETRY_ROUNDS: int = 3
#: seed 标志所在持久化表（db.py 白名单含 dynamic_state）。
_SEED_TABLE: str = "dynamic_state"
#: 动态详情页链接模板（DDBOT DynamicUrl 同款，t.bilibili.com/<dynamic_id>）。
_DYNAMIC_URL_TMPL: str = "https://t.bilibili.com/{}"
#: 未知动态类型的通用文案。
_GENERIC_TYPE_TEXT: str = "发布新动态"

#: 格式化函数签名：``(item, 正文文本) -> (content, cover_url)``。
_Formatter = Callable[[dict[str, Any], str], tuple[str, str]]

_logger: logging.Logger | None = None


async def _noop_acquire() -> None:
    """默认无操作取牌：未注入令牌桶时行为与之前完全一致。"""
    return None


def _get_logger() -> logging.Logger:
    """返回插件统一 logger；离线环境回退 stdlib logger。"""
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger(__name__)
        else:
            _logger = astrbot_logger
    return _logger


def _dig(item: dict[str, Any], *keys: str) -> Any:
    """沿 ``keys`` 逐层取嵌套值；任一层缺失返回 ``None``。"""
    current: Any = item
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _text(value: Any) -> str:
    """标量转去除首尾空白的文本；``None``/容器返回空串。"""
    if value is None or isinstance(value, (dict, list)):
        return ""
    return str(value).strip()


def _join(*parts: str) -> str:
    """用换行连接非空片段。"""
    return "\n".join(p for p in parts if p)


def _major(item: dict[str, Any], major_key: str) -> dict[str, Any]:
    """取新 API ``modules.module_dynamic.major.<major_key>``；缺失返回空 dict。"""
    major = _dig(item, "modules", "module_dynamic", "major", major_key)
    return major if isinstance(major, dict) else {}


def _card(item: dict[str, Any]) -> dict[str, Any]:
    """解析旧 API 的 ``card`` JSON 字符串为 dict；非字符串/解析失败返回空 dict。"""
    card = item.get("card")
    if not isinstance(card, str):
        return {}
    try:
        parsed = json.loads(card)
    except (json.JSONDecodeError, TypeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _title_body_cover(
    item: dict[str, Any], major_key: str, body_keys: tuple[str, ...]
) -> tuple[str, str, str]:
    """统一提取 (标题, 正文, 封面)：新 API ``major`` 优先，旧 API ``card`` 兜底。"""
    major = _major(item, major_key)
    card = _card(item)
    title = _text(major.get("title") or card.get("title"))
    body = ""
    for key in body_keys:
        value = major.get(key) or card.get(key)
        if value:
            body = _text(value)
            break
    cover = _text(major.get("cover") or major.get("pic") or card.get("pic"))
    return title, body, cover


def _major_formatter(major_key: str, body_keys: tuple[str, ...]) -> _Formatter:
    """构造单 major 子键的 (content, cover) 格式化函数（desc 兜底）。"""

    def formatter(item: dict[str, Any], desc: str) -> tuple[str, str]:
        title, body, cover = _title_body_cover(item, major_key, body_keys)
        return _join(title, body) or desc, cover

    return formatter


def _format_draw(item: dict[str, Any], desc: str) -> tuple[str, str]:
    """图片动态：正文取标题/文本；封面取首图 src（新 API ``items`` / 旧 API ``item``）。"""
    major = _major(item, "draw")
    card = _card(item)
    title = _text(major.get("title") or card.get("title"))
    cover = ""
    for source in (major.get("items"), card.get("item")):
        if isinstance(source, list) and source and isinstance(source[0], dict):
            cover = _text(source[0].get("src"))
            break
    return title or desc, cover


def _format_plain(item: dict[str, Any], desc: str) -> tuple[str, str]:
    """文字/转发/未知类型：仅用动态正文文本，无封面。"""
    return desc, ""


#: 动态类型码 -> (类型文案, 内容格式化函数)。
DYNAMIC_TYPE_HANDLERS: dict[int, tuple[str, _Formatter]] = {
    8: ("视频投稿", _major_formatter("archive", ("desc",))),
    2: ("图片", _format_draw),
    4: ("文字", _format_plain),
    1: ("转发", _format_plain),
    64: ("专栏", _major_formatter("article", ("desc", "summary"))),
    256: ("音频", _major_formatter("music", ("intro", "desc"))),
    2048: ("图文", _major_formatter("opus", ("summary", "desc"))),
    4200: ("直播分享", _major_formatter("live", ("desc",))),
    4308: ("直播分享", _major_formatter("live", ("desc",))),
    4300: ("收藏夹", _major_formatter("mylist", ("desc",))),
    4302: ("课程", _major_formatter("course", ("desc",))),
}


class DynamicPoller:
    """动态订阅轮询器：检测并推送 UP 主新动态。

    Args:
        subscription: 规范化后的 dynamic 订阅（uid 非空）。
        repo: :class:`BiliRepository` 实现（或测试 fake）。
        db: 数据层 :class:`Database`（已 init）。
        build_chain: ``push.build_chain``（event_type, payload -> str|MessageChain）。
        send: ``push.send``（subscription, chain, context, status -> dict[str, bool]）。
        context: AstrBot Context（或暴露 ``async send_message(session, chain) -> bool``
            的 fake）。
        status: main.py 持有的 runtime status dict（按 sub_id，可变对象）。
        retry_counts: main.py 持有的重试计数 dict（``{sub_id: {dynamic_id: n}}``），
            跨 poller 重建保留；``initialize()`` 清空即重启后重新计数。
        logger: 显式 logger；缺省用插件统一 logger。
        acquire: 每次 B 站请求前调用的异步取牌函数（调度器注入令牌桶）；
            缺省为无操作，行为不变。
    """

    def __init__(
        self,
        subscription: Subscription,
        repo: BiliRepository,
        db: Database,
        build_chain: Callable[[str, dict[str, Any]], Any],
        send: Callable[
            [Subscription, Any, Any, dict[str, Any]], Awaitable[dict[str, bool]]
        ],
        context: Any,
        status: dict[str, Any],
        retry_counts: dict[str, dict[str, int]],
        logger: logging.Logger | None = None,
        acquire: Callable[[], Awaitable[None]] | None = None,
    ) -> None:
        self.subscription = subscription
        self.repo = repo
        self.db = db
        self.build_chain = build_chain
        self.send = send
        self.context = context
        self.status = status
        self.retry_counts = retry_counts
        self._acquire: Callable[[], Awaitable[None]] = (
            acquire if acquire is not None else _noop_acquire
        )
        self._logger = logger if logger is not None else _get_logger()

    async def poll(self) -> None:
        """执行一轮动态扫描；仓库/未知异常吞掉记日志，不向上抛出。

        ``asyncio.CancelledError`` 透传（任务取消属正常 shutdown 路径）。
        """
        try:
            await self._poll_once()
        except asyncio.CancelledError:
            raise
        except BiliError as exc:
            self._logger.warning(
                "动态轮询失败（sub=%s）: %s", self.subscription.name, exc
            )
        except Exception as exc:  # noqa: BLE001 - 轮询任务不允许未捕获异常
            self._logger.error(
                "动态轮询异常（sub=%s）: %s",
                self.subscription.name,
                exc,
                exc_info=True,
            )

    async def _poll_once(self) -> None:
        sub = self.subscription
        if sub.uid is None:
            self._logger.warning("动态订阅缺少 uid，跳过: %s", sub.name)
            return
        items = await self._fetch_feed(sub.uid)
        if not await self.db.get_seeded(_SEED_TABLE, sub.id):
            await self._seed(sub, items)
            return
        for item in items:
            await self._handle_item(sub, item)

    async def _fetch_feed(self, uid: int) -> list[dict[str, Any]]:
        """从空 offset 拉取动态，翻页直到 ``has_more`` 为假或达页数上限。"""
        collected: list[dict[str, Any]] = []
        offset: str | int = 0
        pages = 0
        while True:
            await self._acquire()
            resp = await self.repo.get_dynamics(uid, offset=offset)
            pages += 1
            items = resp.get("items")
            if isinstance(items, list):
                collected.extend(items)
            if not resp.get("has_more"):
                break
            if pages >= _MAX_PAGES:
                self._logger.warning(
                    "动态扫描已达 %d 页上限（has_more 仍为真），停止翻页: sub=%s",
                    _MAX_PAGES,
                    self.subscription.name,
                )
                break
            offset = resp.get("offset", offset)
        return collected

    async def _seed(self, sub: Subscription, items: list[dict[str, Any]]) -> None:
        """首轮静默 seed：全量写入 known_dynamics（不推送），持久化 seed 标志。"""
        for item in items:
            dyn_id = self._dynamic_id(item)
            if dyn_id:
                await self.db.insert_dynamic_if_new(
                    sub.id, dyn_id, self._dynamic_type(item)
                )
        await self.db.set_seeded(_SEED_TABLE, sub.id, True)

    async def _handle_item(self, sub: Subscription, item: dict[str, Any]) -> None:
        """处理单条动态：去重 → 推送 → 按结果维护 mark/重试计数。

        mark-after-send：``insert_dynamic_if_new`` 即持久化 mark；推送全失败时
        保留重试计数继续重试（达上限后告警并停止，行保留即"仍标记"）；
        任一 session 成功则清除重试计数。
        """
        dyn_id = self._dynamic_id(item)
        if not dyn_id:
            return
        type_ = self._dynamic_type(item)
        retries = self.retry_counts.get(sub.id, {}).get(dyn_id, 0)
        newly = await self.db.insert_dynamic_if_new(sub.id, dyn_id, type_)
        if not newly and retries == 0:
            return  # 已见且无进行中的重试
        chain = self.build_chain("dynamic", self._payload(sub, item, dyn_id, type_))
        results = await self.send(sub, chain, self.context, self.status)
        if any(results.values()):
            if retries:
                self.retry_counts[sub.id].pop(dyn_id, None)
            return
        retries += 1
        self.retry_counts.setdefault(sub.id, {})[dyn_id] = retries
        if retries >= _MAX_RETRY_ROUNDS:
            self.retry_counts[sub.id].pop(dyn_id, None)
            self._logger.warning(
                "动态推送连续 %d 轮失败，已标记为已见不再重试: sub=%s dynamic=%s",
                _MAX_RETRY_ROUNDS,
                sub.name,
                dyn_id,
            )

    def _payload(
        self, sub: Subscription, item: dict[str, Any], dyn_id: str, type_: int
    ) -> dict[str, Any]:
        """构造 dynamic 推送载荷（缺失键防御，封面缺失则不携带 cover）。"""
        handler = DYNAMIC_TYPE_HANDLERS.get(type_)
        if handler is None:
            type_text, formatter = _GENERIC_TYPE_TEXT, _format_plain
        else:
            type_text, formatter = handler
        desc = self._plain_text(item)
        content, cover = formatter(item, desc)
        name = self._author_name(item) or sub.name
        payload: dict[str, Any] = {
            "name": name,
            "type_text": type_text,
            "content": content,
            "event_time": format_event_time(self._dynamic_timestamp(item)),
            "url": _DYNAMIC_URL_TMPL.format(dyn_id),
        }
        if cover:
            payload["cover"] = cover
        return payload

    def _plain_text(self, item: dict[str, Any]) -> str:
        """动态正文文本：新 API ``desc.text``，旧 API ``desc`` 兜底。"""
        text = _text(_dig(item, "modules", "module_dynamic", "desc", "text"))
        if not text:
            desc = item.get("desc")
            if isinstance(desc, dict):
                text = _text(desc.get("text"))
            elif isinstance(desc, str):
                text = desc.strip()
        return text

    def _author_name(self, item: dict[str, Any]) -> str:
        """UP 主名：新 API ``module_author.name``；缺失返回空串。"""
        name = _dig(item, "modules", "module_author", "name")
        return str(name).strip() if name else ""

    def _dynamic_id(self, item: dict[str, Any]) -> str:
        """动态 ID：新 API ``id``，旧 API ``desc.dynamic_id*`` 兜底；取不到返回空串。"""
        raw = _dig(item, "id")
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("dynamic_id_str") or desc.get("dynamic_id")
                if isinstance(raw, dict):
                    raw = raw.get("dynamic_id_str")
        if raw is None:
            return ""
        return str(raw).strip()

    def _dynamic_type(self, item: dict[str, Any]) -> int:
        """动态类型码：新 API ``type``，旧 API ``desc.type`` 兜底；非法返回 0。"""
        raw = _dig(item, "type")
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("type")
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0

    def _dynamic_timestamp(self, item: dict[str, Any]) -> int:
        """动态发布时间戳：新 API ``module_author.pub_ts``，旧 API ``desc.timestamp``。"""
        raw = _dig(item, "modules", "module_author", "pub_ts")
        if raw is None:
            desc = item.get("desc")
            if isinstance(desc, dict):
                raw = desc.get("timestamp")
        if raw is None:
            return 0
        try:
            return int(raw)
        except (TypeError, ValueError):
            return 0
