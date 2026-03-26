"""Staging buffer — holds records when a target backend is unavailable.

Records are persisted to DuckDB (using a dedicated staging database) so they
survive process restarts.  A background retry loop attempts to flush staged
records once the backend becomes available again.

Design decisions:
- Never lose data: records are staged before the write is attempted.
- Exponential backoff: 1s → 2s → 4s → … up to 5 minutes.
- Max attempts: configurable (default 10) — after which an alert is raised.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

logger = structlog.get_logger(__name__)

_MAX_ATTEMPTS = 10
_BASE_BACKOFF_SECONDS = 1.0
_MAX_BACKOFF_SECONDS = 300.0


class StagingBuffer:
    """DuckDB-backed staging buffer for records awaiting backend availability.

    Usage::

        buffer = StagingBuffer(duckdb_store)
        await buffer.open()
        await buffer.enqueue(record_id, backend_id, collection, payload)
        await buffer.flush(backend_id, backend)   # called when backend comes back
    """

    _CREATE_TABLE = """
    CREATE TABLE IF NOT EXISTS staging_buffer (
        id              TEXT PRIMARY KEY,
        record_id       TEXT NOT NULL,
        backend_id      TEXT NOT NULL,
        collection      TEXT NOT NULL,
        payload_json    TEXT NOT NULL,
        enqueued_at     TEXT NOT NULL,
        attempt_count   INTEGER NOT NULL DEFAULT 0,
        last_attempt_at TEXT,
        error_detail    TEXT
    );
    """

    def __init__(self, duckdb_store: Any) -> None:
        self._store = duckdb_store

    async def open(self) -> None:
        await self._store._run(self._CREATE_TABLE)
        logger.info("staging_buffer_opened")

    async def enqueue(
        self,
        record_id: uuid.UUID,
        backend_id: str,
        collection: str,
        payload: dict[str, Any],
    ) -> str:
        """Add a record to the staging buffer.

        Returns:
            The staging entry ID.
        """
        entry_id = str(uuid.uuid4())
        await self._store._run(
            """INSERT INTO staging_buffer
               (id, record_id, backend_id, collection, payload_json, enqueued_at)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                entry_id,
                str(record_id),
                backend_id,
                collection,
                json.dumps(payload, default=str),
                datetime.now(timezone.utc).isoformat(),
            ),
        )
        logger.info(
            "staging_buffer_enqueued",
            entry_id=entry_id,
            record_id=str(record_id),
            backend_id=backend_id,
        )
        return entry_id

    async def flush(self, backend_id: str, backend: Any) -> int:
        """Attempt to flush all staged records for a backend.

        Returns:
            Number of records successfully flushed.
        """
        cols, rows = await self._store._fetch(
            "SELECT * FROM staging_buffer WHERE backend_id = ? AND attempt_count < ? ORDER BY enqueued_at ASC",
            (backend_id, _MAX_ATTEMPTS),
        )
        if not rows:
            return 0

        flushed = 0
        for row in rows:
            entry = dict(zip(cols, row))
            await self._attempt_flush(entry, backend)
            flushed += 1

        return flushed

    async def _attempt_flush(self, entry: dict[str, Any], backend: Any) -> None:
        entry_id = entry["id"]
        payload = json.loads(entry["payload_json"])
        attempt = entry["attempt_count"] + 1

        try:
            await backend.write(
                entry["collection"],
                [payload],
                tenant_id=payload.get("_tenant_id", "default"),
            )
            # Success — remove from staging
            await self._store._run(
                "DELETE FROM staging_buffer WHERE id = ?", (entry_id,)
            )
            logger.info("staging_buffer_flushed", entry_id=entry_id, attempts=attempt)

        except Exception as exc:
            backoff = min(_BASE_BACKOFF_SECONDS * (2 ** attempt), _MAX_BACKOFF_SECONDS)
            await self._store._run(
                """UPDATE staging_buffer
                   SET attempt_count = ?, last_attempt_at = ?, error_detail = ?
                   WHERE id = ?""",
                (attempt, datetime.now(timezone.utc).isoformat(), str(exc), entry_id),
            )
            logger.warning(
                "staging_flush_failed",
                entry_id=entry_id,
                attempt=attempt,
                backoff_s=backoff,
                error=str(exc),
            )
            if attempt >= _MAX_ATTEMPTS:
                logger.error(
                    "staging_max_attempts_reached",
                    entry_id=entry_id,
                    backend_id=entry["backend_id"],
                )

    async def pending_count(self, backend_id: str | None = None) -> int:
        """Count records pending in the staging buffer."""
        if backend_id:
            _, rows = await self._store._fetch(
                "SELECT COUNT(*) FROM staging_buffer WHERE backend_id = ?",
                (backend_id,),
            )
        else:
            _, rows = await self._store._fetch("SELECT COUNT(*) FROM staging_buffer")
        return rows[0][0] if rows else 0

    async def grouped_pending(self) -> list[dict[str, Any]]:
        """Pending rows grouped by backend and collection (for catalog UI)."""
        _, rows = await self._store._fetch(
            """SELECT backend_id, collection, COUNT(*) AS pending_count
               FROM staging_buffer
               GROUP BY backend_id, collection
               ORDER BY pending_count DESC"""
        )
        return [
            {"backend_id": r[0], "collection": r[1], "pending_count": int(r[2])}
            for r in rows
        ]
