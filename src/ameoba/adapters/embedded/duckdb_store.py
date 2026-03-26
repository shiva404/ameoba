"""DuckDB-backed storage backend.

Handles relational data (OLTP writes + OLAP queries) using DuckDB's
in-process columnar engine.  Also serves as the federation compute engine
for cross-backend queries.

Design notes:
- One DuckDB file per Ameoba instance (multi-reader, single-writer).
- Collections are stored as DuckDB tables with schema derived from the data.
- Tenant isolation via a ``tenant_id`` column on all tables.
- DuckDB is also used as the staging buffer database.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb
import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

logger = structlog.get_logger(__name__)


class DuckDBStore:
    """DuckDB storage backend.

    This class satisfies the ``StorageBackend`` protocol.

    Thread safety: DuckDB connections are not thread-safe.  We serialise
    access via an asyncio lock.  For high-throughput scenarios, use a
    dedicated writer process and read replicas.
    """

    BACKEND_ID = "duckdb-embedded"
    SUPPORTED_CATEGORIES = ["relational", "vector"]

    def __init__(self, path: Path) -> None:
        self._path = path
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def open(self) -> None:
        """Open the DuckDB database and install required extensions."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # DuckDB connections are synchronous; we wrap them in the executor
        self._conn = await asyncio.get_event_loop().run_in_executor(
            None, lambda: duckdb.connect(str(self._path))
        )
        await self._run("PRAGMA threads=4")

        logger.info("duckdb_store_opened", path=str(self._path))

    async def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def _assert_open(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("DuckDBStore is not open — call await store.open() first")
        return self._conn

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self.BACKEND_ID,
            display_name="DuckDB (embedded)",
            tier=BackendTier.EMBEDDED,
            status=BackendStatus.AVAILABLE,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"path": str(self._path)},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self.BACKEND_ID,
            supports_predicate_pushdown=True,
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=True,
            supports_sort_pushdown=True,
            supports_limit_pushdown=True,
            supports_joins=True,
            native_language="sql",
        )

    async def health_check(self) -> BackendStatus:
        try:
            await self._run("SELECT 1")
            return BackendStatus.AVAILABLE
        except Exception:
            return BackendStatus.UNAVAILABLE

    async def write(
        self,
        collection: str,
        records: list[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> list[str]:
        """Write records to a DuckDB table, creating it if necessary.

        Returns the list of assigned record IDs.
        """
        if not records:
            return []

        # Assign IDs
        enriched: list[dict[str, Any]] = []
        record_ids: list[str] = []
        for r in records:
            rid = r.get("_id") or str(uuid.uuid4())
            row = {
                "_id": rid,
                "_tenant_id": tenant_id,
                "_ingested_at": _utcnow_iso(),
                **{k: v for k, v in r.items() if not k.startswith("_")},
            }
            enriched.append(row)
            record_ids.append(rid)

        table = _safe_table_name(collection)
        await self._ensure_table(table, enriched[0])
        await self._insert_rows(table, enriched)

        return record_ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        table = _safe_table_name(collection)
        if not await self._table_exists(table):
            return None

        columns, rows = await self._fetch(
            f'SELECT * FROM "{table}" WHERE _id = ? AND _tenant_id = ? LIMIT 1',
            (record_id, tenant_id),
        )
        if not rows:
            return None
        return dict(zip(columns, rows[0]))

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        """Execute a native DuckDB SQL sub-plan."""
        sql = str(sub_plan.native_query)
        t0 = asyncio.get_event_loop().time()
        result = await self._fetch(sql)
        elapsed_ms = (asyncio.get_event_loop().time() - t0) * 1000
        return QueryResult(
            columns=result[0],
            rows=result[1],
            row_count=len(result[1]),
            backend_ids_used=[self.BACKEND_ID],
            execution_ms=elapsed_ms,
        )

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        _, rows = await self._fetch("SHOW TABLES;")
        return [row[0] for row in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _run(self, sql: str, params: tuple = ()) -> None:
        """Execute a non-SELECT SQL statement (DDL / DML)."""
        conn = self._assert_open()

        def _exec() -> None:
            try:
                conn.execute(sql, list(params))
            except duckdb.Error as e:
                logger.error("duckdb_query_error", sql=sql[:200], error=str(e))
                raise

        async with self._lock:
            await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def _fetch(
        self, sql: str, params: tuple = ()
    ) -> tuple[list[str], list[list[Any]]]:
        """Execute a SELECT SQL statement and return (columns, rows).

        DuckDB 1.x: after conn.execute() the connection acts as a cursor.
        Column names are read from conn.description (list of 7-tuples, first
        element is the column name).
        """
        conn = self._assert_open()

        def _exec() -> tuple[list[str], list[list[Any]]]:
            try:
                conn.execute(sql, list(params))
                desc = conn.description  # [(name, type_code, ...)] or None
                if not desc:
                    return [], []
                columns = [d[0] for d in desc]
                rows = conn.fetchall()
                return columns, [list(r) for r in rows]
            except duckdb.Error as e:
                logger.error("duckdb_fetch_error", sql=sql[:200], error=str(e))
                raise

        async with self._lock:
            return await asyncio.get_event_loop().run_in_executor(None, _exec)

    async def _table_exists(self, table: str) -> bool:
        _, rows = await self._fetch(
            "SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?",
            (table,),
        )
        return bool(rows and rows[0][0] > 0)

    async def _ensure_table(self, table: str, sample_row: dict[str, Any]) -> None:
        """Create the table if it does not exist, inferring column types."""
        if await self._table_exists(table):
            return

        col_defs = _infer_columns(sample_row)
        ddl = f"CREATE TABLE IF NOT EXISTS {table} ({col_defs});"
        await self._run(ddl)
        logger.info("duckdb_table_created", table=table)

    async def _insert_rows(self, table: str, rows: list[dict[str, Any]]) -> None:
        """Batch-insert rows using DuckDB's VALUES clause."""
        if not rows:
            return

        cols = list(rows[0].keys())
        placeholders = ", ".join(["?" for _ in cols])
        col_list = ", ".join([f'"{c}"' for c in cols])
        sql = f"INSERT INTO {table} ({col_list}) VALUES ({placeholders})"

        conn = self._assert_open()

        def _batch_insert() -> None:
            conn.executemany(sql, [
                [_serialise_value(row.get(c)) for c in cols]
                for row in rows
            ])

        async with self._lock:
            await asyncio.get_event_loop().run_in_executor(None, _batch_insert)

    async def execute_sql(self, sql: str, params: tuple = ()) -> QueryResult:
        """Execute arbitrary SQL directly — used by the federation engine."""
        result = await self._fetch(sql, params)
        return QueryResult(
            columns=result[0],
            rows=result[1],
            row_count=len(result[1]),
            backend_ids_used=[self.BACKEND_ID],
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_table_name(collection: str) -> str:
    """Sanitise a collection name for use as a DuckDB table name."""
    return "".join(c if c.isalnum() or c == "_" else "_" for c in collection).lower()


def _infer_columns(sample: dict[str, Any]) -> str:
    """Generate a CREATE TABLE column definition string from a sample row."""
    type_map: dict[type, str] = {
        int: "BIGINT",
        float: "DOUBLE",
        bool: "BOOLEAN",
        str: "VARCHAR",
        bytes: "BLOB",
    }
    parts: list[str] = []
    for col, val in sample.items():
        duck_type = type_map.get(type(val), "VARCHAR")
        parts.append(f'"{col}" {duck_type}')
    return ", ".join(parts)


def _serialise_value(v: Any) -> Any:
    """Convert Python values to DuckDB-compatible types."""
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    if isinstance(v, bytes):
        return v
    return v
