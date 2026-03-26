"""Schema registry — versioned, immutable schema store backed by DuckDB.

Each schema version is stored once and never modified.  Records reference
their schema version at ingest time.  The query engine uses version-aware
deserialization to query across schema versions.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ameoba.domain.schema import SchemaCompatibility, SchemaVersion
from ameoba.schema.compatibility import check_compatibility

logger = structlog.get_logger(__name__)

_CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS schema_registry (
    id                      TEXT PRIMARY KEY,
    collection              TEXT NOT NULL,
    version_number          INTEGER NOT NULL,
    json_schema_json        TEXT NOT NULL,
    inferred_category       TEXT NOT NULL,
    field_count             INTEGER NOT NULL DEFAULT 0,
    nesting_depth           INTEGER NOT NULL DEFAULT 0,
    key_consistency_score   DOUBLE NOT NULL DEFAULT 0.0,
    complexity_score        DOUBLE NOT NULL DEFAULT 0.0,
    created_at              TEXT NOT NULL,
    record_count_at_inference INTEGER NOT NULL DEFAULT 0,
    previous_version_id     TEXT,
    compatibility           TEXT NOT NULL DEFAULT 'unknown',
    UNIQUE(collection, version_number)
);
"""

_CREATE_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS idx_schema_registry_collection
    ON schema_registry (collection, version_number DESC);
"""


class SchemaRegistry:
    """DuckDB-backed schema version store.

    Usage::

        registry = SchemaRegistry(duckdb_store)
        await registry.open()

        version = await registry.register(
            collection="users",
            json_schema=inferred_schema,
            category="relational",
            records=sample_records,
        )
    """

    def __init__(self, duckdb_store: Any) -> None:  # DuckDBStore
        self._store = duckdb_store
        self._lock = asyncio.Lock()

    async def open(self) -> None:
        """Create the schema registry table if it doesn't exist."""
        await self._store._run(_CREATE_TABLE_SQL)
        await self._store._run(_CREATE_INDEX_SQL)
        logger.info("schema_registry_opened")

    async def register(
        self,
        collection: str,
        json_schema: dict[str, Any],
        category: str,
        *,
        records: list[dict[str, Any]] | None = None,
    ) -> SchemaVersion:
        """Register a new schema version for a collection.

        If the schema is identical to the latest version, returns the existing
        version without creating a duplicate.

        Returns:
            The (possibly new) SchemaVersion.
        """
        from ameoba.schema.inference import compute_schema_metrics

        metrics = compute_schema_metrics(json_schema, records or [])

        async with self._lock:
            latest = await self._latest(collection)

            if latest is not None:
                compat = check_compatibility(latest.json_schema, json_schema)
                if compat == SchemaCompatibility.IDENTICAL:
                    return latest  # No change — return existing version

                version_number = latest.version_number + 1
                prev_id = latest.id
            else:
                compat = SchemaCompatibility.UNKNOWN
                version_number = 1
                prev_id = None

            version = SchemaVersion(
                id=uuid.uuid4(),
                collection=collection,
                version_number=version_number,
                json_schema=json_schema,
                inferred_category=category,
                field_count=int(metrics["field_count"]),
                nesting_depth=int(metrics["nesting_depth"]),
                key_consistency_score=metrics["key_consistency_score"],
                complexity_score=metrics["complexity_score"],
                created_at=datetime.now(timezone.utc),
                record_count_at_inference=len(records) if records else 0,
                previous_version_id=prev_id,
                compatibility=compat,
            )

            await self._store._run(
                """
                INSERT INTO schema_registry (
                    id, collection, version_number, json_schema_json,
                    inferred_category, field_count, nesting_depth,
                    key_consistency_score, complexity_score, created_at,
                    record_count_at_inference, previous_version_id, compatibility
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    str(version.id),
                    version.collection,
                    version.version_number,
                    json.dumps(version.json_schema),
                    version.inferred_category,
                    version.field_count,
                    version.nesting_depth,
                    version.key_consistency_score,
                    version.complexity_score,
                    version.created_at.isoformat(),
                    version.record_count_at_inference,
                    str(version.previous_version_id) if version.previous_version_id else None,
                    version.compatibility.value,
                ),
            )

            logger.info(
                "schema_version_registered",
                collection=collection,
                version=version_number,
                compatibility=compat.value,
                fields=version.field_count,
            )
            return version

    async def register_from_records(
        self,
        collection: str,
        records: list[dict],
        *,
        category: str = "unknown",
    ) -> SchemaVersion:
        """Infer schema from records and register it.

        Convenience wrapper for the common kernel ingest path.
        """
        from ameoba.schema.inference import infer_schema
        json_schema = infer_schema(records)
        return await self.register(collection, json_schema, category, records=records)

    async def get_latest(self, collection: str) -> SchemaVersion | None:
        """Return the latest schema version for a collection."""
        return await self._latest(collection)

    async def get_version(self, version_id: uuid.UUID) -> SchemaVersion | None:
        """Return a specific schema version by ID."""
        cols, rows = await self._store._fetch(
            "SELECT * FROM schema_registry WHERE id = ? LIMIT 1",
            (str(version_id),),
        )
        if not rows:
            return None
        return _row_to_version(dict(zip(cols, rows[0])))

    async def list_versions(self, collection: str) -> list[SchemaVersion]:
        """Return all schema versions for a collection, newest first."""
        cols, rows = await self._store._fetch(
            "SELECT * FROM schema_registry WHERE collection = ? ORDER BY version_number DESC",
            (collection,),
        )
        return [_row_to_version(dict(zip(cols, row))) for row in rows]

    async def list_collections(self) -> list[str]:
        """Return all collection names that have registered schemas."""
        _, rows = await self._store._fetch(
            "SELECT DISTINCT collection FROM schema_registry ORDER BY collection"
        )
        return [r[0] for r in rows]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    async def _latest(self, collection: str) -> SchemaVersion | None:
        cols, rows = await self._store._fetch(
            """SELECT * FROM schema_registry
               WHERE collection = ?
               ORDER BY version_number DESC
               LIMIT 1""",
            (collection,),
        )
        if not rows:
            return None
        return _row_to_version(dict(zip(cols, rows[0])))


def _row_to_version(row: dict[str, Any]) -> SchemaVersion:
    return SchemaVersion(
        id=uuid.UUID(row["id"]),
        collection=row["collection"],
        version_number=row["version_number"],
        json_schema=json.loads(row["json_schema_json"]),
        inferred_category=row["inferred_category"],
        field_count=row["field_count"],
        nesting_depth=row["nesting_depth"],
        key_consistency_score=row["key_consistency_score"],
        complexity_score=row["complexity_score"],
        created_at=datetime.fromisoformat(row["created_at"]),
        record_count_at_inference=row["record_count_at_inference"],
        previous_version_id=uuid.UUID(row["previous_version_id"]) if row.get("previous_version_id") else None,
        compatibility=SchemaCompatibility(row["compatibility"]),
    )
