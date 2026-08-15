"""Async SQLite data layer for the Bilibili monitor plugin.

A single shared :mod:`aiosqlite` connection with WAL journaling backs all
persistence. Every table is keyed by the subscription ``sub_id`` so that
multiple subscriptions pointing at the same uid stay fully independent.

The default database location is ``<astrbot data dir>/plugin_data/
astrbot_plugin_bilibili_cj/state.db``; it is resolved lazily so that offline tests
can point the :class:`Database` at a temporary file without importing AstrBot.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Final

import aiosqlite

_PLUGIN_DATA_DIR_NAME: Final = "astrbot_plugin_bilibili_cj"
_DB_FILE_NAME: Final = "state.db"

#: Newest dynamics rows kept per subscription when pruning.
_PRUNE_KEEP_PER_SUB: Final = 1000
#: Upper bound of items a single dynamics feed page can return (Bilibili API cap).
_DYNAMICS_PAGE_MAX: Final = 50

#: live_state columns allowed in upsert_live_state (guards against SQL injection
#: through the **fields keyword arguments).
_LIVE_STATE_COLUMNS: Final = frozenset(
    {
        "uid",
        "room_id",
        "last_status",
        "last_title",
        "last_live_time",
        "live_ended_at",
        "consecutive_offline_count",
        "last_checked_at",
        "pending_push",
        "offline_notified",
    }
)

#: Tables holding per-subscription seed flags.
#
# v1 表（``dynamic_state``/``collection_state``）是动态解析缺陷修复前的遗留
# 标记：旧解析器在未写入任何去重记录时也会置位 seeded，修复后若沿用会造成
# 「升级后洪水推送历史内容」。v2 表作为新一代种子标记，旧标记一律失效——
# 历史部署升级后首轮会静默重 seed（记录当前可见内容、不推送），从而根治洪水。
_SEED_TABLES: Final = frozenset(
    {"dynamic_state", "collection_state", "dynamic_state_v2", "collection_state_v2"}
)

#: All tables carrying a sub_id column (used by delete_sub_state).
_ALL_TABLES: Final = (
    "known_dynamics",
    "known_videos",
    "live_state",
    "dynamic_state",
    "collection_state",
    "dynamic_state_v2",
    "collection_state_v2",
)

_DDL: Final = (
    """CREATE TABLE IF NOT EXISTS known_dynamics (
        sub_id TEXT,
        dynamic_id TEXT,
        type INTEGER,
        seen_at TEXT,
        PRIMARY KEY (sub_id, dynamic_id)
    )""",
    """CREATE TABLE IF NOT EXISTS known_videos (
        sub_id TEXT,
        bvid TEXT,
        uid INTEGER,
        list_id INTEGER,
        seen_at TEXT,
        PRIMARY KEY (sub_id, bvid)
    )""",
    """CREATE TABLE IF NOT EXISTS live_state (
        sub_id TEXT PRIMARY KEY,
        uid INTEGER,
        room_id INTEGER,
        last_status INTEGER,
        last_title TEXT,
        last_live_time INTEGER,
        live_ended_at INTEGER,
        consecutive_offline_count INTEGER,
        last_checked_at TEXT,
        pending_push TEXT,
        offline_notified INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS dynamic_state (
        sub_id TEXT PRIMARY KEY,
        seeded INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS collection_state (
        sub_id TEXT PRIMARY KEY,
        seeded INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS dynamic_state_v2 (
        sub_id TEXT PRIMARY KEY,
        seeded INTEGER
    )""",
    """CREATE TABLE IF NOT EXISTS collection_state_v2 (
        sub_id TEXT PRIMARY KEY,
        seeded INTEGER
    )""",
)

_logger: logging.Logger | None = None


def _get_logger() -> logging.Logger:
    """Return the AstrBot plugin logger when running inside AstrBot, else stdlib.

    Kept lazy so importing this module never depends on AstrBot being installed
    (offline tests and the smoke test use the stdlib fallback).
    """
    global _logger
    if _logger is None:
        try:
            from astrbot.api import logger as astrbot_logger  # type: ignore[import-not-found]
        except ImportError:
            _logger = logging.getLogger(__name__)
        else:
            _logger = astrbot_logger
    return _logger


def _now_iso() -> str:
    """Return the current UTC time as an ISO-8601 string (lexicographically sortable)."""
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


@dataclass(frozen=True, slots=True)
class LiveState:
    """Snapshot of the persisted live-polling state for one subscription.

    Attributes mirror the ``live_state`` table columns; ``pending_push`` is the
    raw retry payload JSON (``None`` when there is none).
    """

    sub_id: str
    uid: int | None = None
    room_id: int | None = None
    last_status: int | None = None
    last_title: str | None = None
    last_live_time: int | None = None
    live_ended_at: int | None = None
    consecutive_offline_count: int | None = None
    last_checked_at: str | None = None
    pending_push: str | None = None
    offline_notified: int | None = None


class Database:
    """Async SQLite persistence for the plugin.

    All reads and writes go through a single aiosqlite connection, serialized by
    an :class:`asyncio.Lock` so concurrent poller tasks cannot interleave
    execute/fetch sequences. The connection is opened (and tables created) by
    :meth:`init`, which is idempotent.

    Args:
        db_path: Explicit SQLite file path. When omitted, resolves lazily to
            ``<astrbot data dir>/plugin_data/astrbot_plugin_bilibili_cj/state.db``.
    """

    def __init__(self, db_path: str | Path | None = None) -> None:
        self.db_path: Path = (
            Path(db_path) if db_path is not None else self._default_path()
        )
        self._conn: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    @staticmethod
    def _default_path() -> Path:
        """Resolve the production data path from AstrBot's data directory.

        Falls back to a ``./data/plugin_data/...`` convention when AstrBot is
        not importable (offline contexts).
        """
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
        except ImportError:
            _get_logger().warning(
                "get_astrbot_data_path unavailable; using ./data/plugin_data/%s",
                _PLUGIN_DATA_DIR_NAME,
            )
            return Path("data") / "plugin_data" / _PLUGIN_DATA_DIR_NAME / _DB_FILE_NAME
        return (
            Path(get_astrbot_data_path())
            / "plugin_data"
            / _PLUGIN_DATA_DIR_NAME
            / _DB_FILE_NAME
        )

    async def init(self) -> None:
        """Open the connection, apply PRAGMAs, create tables and migrate.

        Idempotent: a second call while already initialized is a no-op, so both
        fresh installs and plugin reloads can call it safely.

        Raises:
            sqlite3.OperationalError: When the database cannot be opened.
        """
        async with self._lock:
            if self._conn is not None:
                return
            self.db_path.parent.mkdir(parents=True, exist_ok=True)
            conn = await aiosqlite.connect(self.db_path, isolation_level=None)
            conn.row_factory = sqlite3.Row
            await conn.execute("PRAGMA journal_mode=WAL")
            await conn.execute("PRAGMA busy_timeout=30000")
            for ddl in _DDL:
                await conn.execute(ddl)
            await self._migrate(conn)
            self._conn = conn

    @staticmethod
    async def _migrate(conn: aiosqlite.Connection) -> None:
        """Ensure ``live_state`` carries the ``offline_notified`` column.

        New databases get it via the CREATE TABLE DDL; older databases that
        predate the column get it added here. The existence check makes the
        migration idempotent so re-running it on a fresh database does not fail.
        """
        cursor = await conn.execute("PRAGMA table_info(live_state)")
        columns = {row["name"] for row in await cursor.fetchall()}
        if "offline_notified" not in columns:
            await conn.execute(
                "ALTER TABLE live_state "
                "ADD COLUMN offline_notified INTEGER NOT NULL DEFAULT 0"
            )

    async def close(self) -> None:
        """Close the underlying connection. Idempotent.

        Safe to call after :meth:`init`; calling it before :meth:`init` is a
        no-op.
        """
        async with self._lock:
            conn, self._conn = self._conn, None
            if conn is not None:
                await conn.close()

    def _require_conn(self) -> aiosqlite.Connection:
        """Return the open connection, or raise when :meth:`init` was not called."""
        if self._conn is None:
            raise RuntimeError("Database is not initialized; call init() first")
        return self._conn

    async def insert_dynamic_if_new(
        self, sub_id: str, dynamic_id: str, type_: int
    ) -> bool:
        """Record a seen dynamic; return True only for the first occurrence.

        Args:
            sub_id: Subscription id owning the row.
            dynamic_id: Bilibili dynamic id (string form).
            type_: Bilibili dynamic type code.

        Returns:
            True when the row was newly inserted, False when it already existed.
        """
        async with self._lock:
            cursor = await self._require_conn().execute(
                "INSERT OR IGNORE INTO known_dynamics "
                "(sub_id, dynamic_id, type, seen_at) VALUES (?, ?, ?, ?)",
                (sub_id, dynamic_id, type_, _now_iso()),
            )
            return cursor.rowcount > 0

    async def insert_video_if_new(
        self, sub_id: str, bvid: str, uid: int, list_id: int
    ) -> bool:
        """Record a seen collection video; return True only for the first occurrence.

        Args:
            sub_id: Subscription id owning the row.
            bvid: Bilibili video id.
            uid: Uploader uid.
            list_id: Collection (series) id.

        Returns:
            True when the row was newly inserted, False when it already existed.
        """
        async with self._lock:
            cursor = await self._require_conn().execute(
                "INSERT OR IGNORE INTO known_videos "
                "(sub_id, bvid, uid, list_id, seen_at) VALUES (?, ?, ?, ?, ?)",
                (sub_id, bvid, uid, list_id, _now_iso()),
            )
            return cursor.rowcount > 0

    async def get_live_state(self, sub_id: str) -> LiveState | None:
        """Fetch the persisted live-polling state for a subscription.

        Args:
            sub_id: Subscription id.

        Returns:
            A :class:`LiveState` snapshot, or None when the subscription has no
            state yet.
        """
        async with self._lock:
            cursor = await self._require_conn().execute(
                "SELECT * FROM live_state WHERE sub_id = ?", (sub_id,)
            )
            row = await cursor.fetchone()
            if row is None:
                return None
            return LiveState(**dict(row))

    async def upsert_live_state(self, sub_id: str, **fields: int | str | None) -> None:
        """Insert or update the live_state row for a subscription.

        Only the columns given in ``fields`` are written; pass ``None`` to store
        an explicit NULL. The row is created on first use and updated on later
        calls without clobbering unmentioned columns.

        Args:
            sub_id: Subscription id (primary key).
            **fields: live_state column values keyed by column name.

        Raises:
            ValueError: When a field name is not a live_state column.
        """
        unknown = set(fields) - _LIVE_STATE_COLUMNS
        if unknown:
            raise ValueError(f"unknown live_state columns: {sorted(unknown)}")
        if not fields:
            return
        async with self._lock:
            columns = ", ".join(fields)
            placeholders = ", ".join("?" for _ in fields)
            updates = ", ".join(f"{col} = excluded.{col}" for col in fields)
            await self._require_conn().execute(
                f"INSERT INTO live_state (sub_id, {columns}) VALUES (?, {placeholders}) "
                f"ON CONFLICT(sub_id) DO UPDATE SET {updates}",
                (sub_id, *fields.values()),
            )

    async def reset_offline_count(self, sub_id: str) -> None:
        """Reset the consecutive-offline counter and the offline-pushed flag.

        Called on a 0/2 -> 1 transition so a later 1 -> 0/2 cannot emit a
        duplicate offline notification immediately.

        Args:
            sub_id: Subscription id.
        """
        async with self._lock:
            await self._require_conn().execute(
                "UPDATE live_state "
                "SET consecutive_offline_count = 0, offline_notified = 0 "
                "WHERE sub_id = ?",
                (sub_id,),
            )

    async def get_seeded(self, table: str, sub_id: str) -> bool:
        """Read the persisted seed flag for dynamic_state or collection_state.

        Args:
            table: One of ``"dynamic_state"`` or ``"collection_state"``.
            sub_id: Subscription id.

        Returns:
            True when the subscription finished its silent first scan.

        Raises:
            ValueError: When table is not a seed-state table.
        """
        self._check_seed_table(table)
        async with self._lock:
            cursor = await self._require_conn().execute(
                f"SELECT seeded FROM {table} WHERE sub_id = ?", (sub_id,)
            )
            row = await cursor.fetchone()
            return bool(row[0]) if row is not None else False

    async def set_seeded(self, table: str, sub_id: str, value: bool) -> None:
        """Persist the seed flag for a subscription.

        Args:
            table: One of ``"dynamic_state"`` or ``"collection_state"``.
            sub_id: Subscription id.
            value: New seed flag.

        Raises:
            ValueError: When table is not a seed-state table.
        """
        self._check_seed_table(table)
        async with self._lock:
            await self._require_conn().execute(
                f"INSERT OR REPLACE INTO {table} (sub_id, seeded) VALUES (?, ?)",
                (sub_id, 1 if value else 0),
            )

    @staticmethod
    def _check_seed_table(table: str) -> None:
        """Validate a seed-table name against the whitelist."""
        if table not in _SEED_TABLES:
            raise ValueError(f"unknown seed table: {table!r}")

    async def delete_sub_state(self, sub_id: str) -> None:
        """Remove every stored row for a subscription across all tables.

        Used by the config hot-reload path when a subscription's identity
        (type/uid/list) changes, forcing a fresh seed.

        Args:
            sub_id: Subscription id to purge.
        """
        async with self._lock:
            conn = self._require_conn()
            for table in _ALL_TABLES:
                await conn.execute(f"DELETE FROM {table} WHERE sub_id = ?", (sub_id,))

    async def prune_old(self) -> None:
        """Prune dedup history, keeping the newest rows per subscription.

        ``known_videos`` is intentionally untouched: its row count per
        subscription is bounded by the collection size. ``known_dynamics`` keeps
        the newest ``_PRUNE_KEEP_PER_SUB`` rows per sub_id; pruned rows are
        necessarily outside the poller's 10-page scan window, so they can never
        be re-scanned and re-pushed.
        """
        if _PRUNE_KEEP_PER_SUB <= 10 * _DYNAMICS_PAGE_MAX:
            _get_logger().warning(
                "known_dynamics prune window %d is not > 10 x page max %d; "
                "pruned rows may fall inside the scan window and be re-pushed",
                _PRUNE_KEEP_PER_SUB,
                10 * _DYNAMICS_PAGE_MAX,
            )
        async with self._lock:
            await self._require_conn().execute(
                """DELETE FROM known_dynamics
                   WHERE (sub_id, dynamic_id) IN (
                       SELECT sub_id, dynamic_id
                       FROM (
                           SELECT sub_id, dynamic_id,
                                  ROW_NUMBER() OVER (
                                      PARTITION BY sub_id
                                      ORDER BY seen_at DESC, rowid DESC
                                  ) AS rn
                           FROM known_dynamics
                       )
                       WHERE rn > ?
                   )""",
                (_PRUNE_KEEP_PER_SUB,),
            )
