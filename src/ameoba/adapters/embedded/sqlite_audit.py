"""SQLite-backed audit sink.

Provides the zero-infrastructure append-only audit ledger using aiosqlite.

Tamper-resistance layers implemented here:
1. INSERT-only trigger (BEFORE UPDATE/DELETE raises exception)
2. Gapless sequence validation on read

The Merkle tree lives in the in-process AuditLedger; this adapter
is responsible only for durable persistence of the hashed events.
"""

from __future__ import annotations

import asyncio
import json
import logging
from pathlib import Path
from typing import AsyncIterator

import aiosqlite
import structlog

from ameoba.domain.audit import AuditEvent, AuditEventKind, MerkleNode

logger = structlog.get_logger(__name__)

# Schema is intentionally flat — stability is critical for an audit ledger.
_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS audit_log (
    sequence       INTEGER PRIMARY KEY,   -- gapless monotonic sequence
    id             TEXT    NOT NULL UNIQUE,
    kind           TEXT    NOT NULL,
    occurred_at    TEXT    NOT NULL,
    agent_id       TEXT,
    session_id     TEXT,
    tenant_id      TEXT    NOT NULL DEFAULT 'default',
    record_id      TEXT,
    collection     TEXT,
    backend_id     TEXT,
    detail_json    TEXT    NOT NULL DEFAULT '{}',
    previous_hash  TEXT    NOT NULL,
    event_hash     TEXT    NOT NULL
) STRICT;
"""

# Trigger: block any UPDATE or DELETE at the database level.
_CREATE_UPDATE_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS trg_audit_no_update
BEFORE UPDATE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: UPDATE is forbidden');
END;
"""

_CREATE_DELETE_TRIGGER_SQL = """
CREATE TRIGGER IF NOT EXISTS trg_audit_no_delete
BEFORE DELETE ON audit_log
BEGIN
    SELECT RAISE(ABORT, 'audit_log is append-only: DELETE is forbidden');
END;
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_audit_log_tenant_seq
    ON audit_log (tenant_id, sequence);
"""


class SQLiteAuditSink:
    """Append-only audit ledger backed by a local SQLite database.

    This class satisfies the ``AuditSink`` protocol.

    Usage::

        sink = SQLiteAuditSink(path=settings.embedded.sqlite_audit_path)
        await sink.open()
        event = await sink.append(event)
        ...
        await sink.close()
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._db: aiosqlite.Connection | None = None
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        """Open (or create) the SQLite database and apply the schema."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._db = await aiosqlite.connect(str(self._path))
        self._db.row_factory = aiosqlite.Row

        # WAL mode for better concurrent read performance
        await self._db.execute("PRAGMA journal_mode=WAL;")
        await self._db.execute("PRAGMA synchronous=FULL;")  # fsync on every write
        await self._db.execute("PRAGMA foreign_keys=ON;")

        await self._db.execute(_CREATE_TABLE_SQL)
        await self._db.execute(_CREATE_UPDATE_TRIGGER_SQL)
        await self._db.execute(_CREATE_DELETE_TRIGGER_SQL)
        await self._db.execute(_CREATE_INDEX_SQL)
        await self._db.commit()

        logger.info("sqlite_audit_sink_opened", path=str(self._path))

    async def close(self) -> None:
        if self._db:
            await self._db.close()
            self._db = None

    def _assert_open(self) -> aiosqlite.Connection:
        if self._db is None:
            raise RuntimeError("SQLiteAuditSink is not open — call await sink.open() first")
        return self._db

    async def iter_events_ordered(self) -> AsyncIterator[AuditEvent]:
        """Yield all persisted events in sequence order (for ledger hydration)."""
        db = self._assert_open()
        async with db.execute("SELECT * FROM audit_log ORDER BY sequence ASC") as cursor:
            async for row in cursor:
                yield _row_to_event(row)

    async def append(self, event: AuditEvent) -> AuditEvent:
        """Persist an enriched event (sequence + hashes must already be set)."""
        db = self._assert_open()

        if event.sequence is None or event.event_hash is None:
            raise ValueError("Event must have sequence and event_hash set before appending")

        async with self._lock:
            await db.execute(
                """
                INSERT INTO audit_log (
                    sequence, id, kind, occurred_at,
                    agent_id, session_id, tenant_id,
                    record_id, collection, backend_id,
                    detail_json, previous_hash, event_hash
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    event.sequence,
                    str(event.id),
                    event.kind.value,
                    event.occurred_at.isoformat(),
                    event.agent_id,
                    event.session_id,
                    event.tenant_id,
                    str(event.record_id) if event.record_id else None,
                    event.collection,
                    event.backend_id,
                    json.dumps(event.detail, default=str),
                    event.previous_hash,
                    event.event_hash,
                ),
            )
            await db.commit()

        return event

    async def get_root_hash(self) -> str:
        """Return the event_hash of the latest event (last in chain)."""
        db = self._assert_open()
        async with db.execute(
            "SELECT event_hash FROM audit_log ORDER BY sequence DESC LIMIT 1"
        ) as cursor:
            row = await cursor.fetchone()
        return row["event_hash"] if row else ""

    async def verify_integrity(self) -> tuple[bool, str]:
        """Validate gapless sequences and the hash chain end-to-end.

        Scans the full table — intended for background verification, not
        hot-path use.
        """
        db = self._assert_open()

        prev_hash: str | None = None
        expected_seq = 1
        checked = 0

        async with db.execute(
            "SELECT sequence, previous_hash, event_hash FROM audit_log ORDER BY sequence ASC"
        ) as cursor:
            async for row in cursor:
                seq = row["sequence"]
                if seq != expected_seq:
                    return False, f"Sequence gap detected: expected {expected_seq}, got {seq}"

                if prev_hash is not None and row["previous_hash"] != prev_hash:
                    return (
                        False,
                        f"Hash chain broken at sequence {seq}: "
                        f"previous_hash mismatch",
                    )

                prev_hash = row["event_hash"]
                expected_seq += 1
                checked += 1

        return True, f"Integrity verified: {checked} events, chain intact"

    async def tail(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        tenant_id: str | None = None,
    ) -> AsyncIterator[AuditEvent]:
        """Yield events in sequence order, starting after ``after_sequence``."""
        db = self._assert_open()

        if tenant_id:
            sql = (
                "SELECT * FROM audit_log "
                "WHERE sequence > ? AND tenant_id = ? "
                "ORDER BY sequence ASC LIMIT ?"
            )
            params: tuple = (after_sequence, tenant_id, limit)
        else:
            sql = (
                "SELECT * FROM audit_log "
                "WHERE sequence > ? "
                "ORDER BY sequence ASC LIMIT ?"
            )
            params = (after_sequence, limit)

        async with db.execute(sql, params) as cursor:
            async for row in cursor:
                yield _row_to_event(row)

    async def get_inclusion_proof(self, sequence: int) -> list[MerkleNode]:
        """Not implemented in SQLite sink — proof lives in the in-memory tree."""
        raise NotImplementedError(
            "Merkle inclusion proofs are managed by the in-process AuditLedger, "
            "not the SQLite sink directly."
        )

    async def count(self, *, tenant_id: str | None = None) -> int:
        """Total number of stored events."""
        db = self._assert_open()
        if tenant_id:
            async with db.execute(
                "SELECT COUNT(*) FROM audit_log WHERE tenant_id = ?", (tenant_id,)
            ) as cur:
                row = await cur.fetchone()
        else:
            async with db.execute("SELECT COUNT(*) FROM audit_log") as cur:
                row = await cur.fetchone()
        return row[0] if row else 0

    async def count_by_kind(self, *, tenant_id: str | None = None) -> dict[str, int]:
        """Event counts grouped by ``kind`` (for catalog / ops dashboards)."""
        db = self._assert_open()
        if tenant_id:
            sql = "SELECT kind, COUNT(*) FROM audit_log WHERE tenant_id = ? GROUP BY kind"
            params: tuple = (tenant_id,)
        else:
            sql = "SELECT kind, COUNT(*) FROM audit_log GROUP BY kind"
            params = ()
        out: dict[str, int] = {}
        async with db.execute(sql, params) as cur:
            async for row in cur:
                out[str(row[0])] = int(row[1])
        return out


def _row_to_event(row: aiosqlite.Row) -> AuditEvent:
    """Convert a SQLite row to an AuditEvent domain object."""
    import uuid
    from datetime import datetime

    return AuditEvent(
        id=uuid.UUID(row["id"]),
        sequence=row["sequence"],
        kind=AuditEventKind(row["kind"]),
        occurred_at=datetime.fromisoformat(row["occurred_at"]),
        agent_id=row["agent_id"],
        session_id=row["session_id"],
        tenant_id=row["tenant_id"],
        record_id=uuid.UUID(row["record_id"]) if row["record_id"] else None,
        collection=row["collection"],
        backend_id=row["backend_id"],
        detail=json.loads(row["detail_json"]),
        previous_hash=row["previous_hash"],
        event_hash=row["event_hash"],
    )
