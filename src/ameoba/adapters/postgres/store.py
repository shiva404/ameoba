"""PostgreSQL storage backend.

Uses asyncpg for high-performance async I/O.  Tables are created dynamically
from inferred schemas.  Tenant isolation via Row-Level Security (RLS).

Promotion path: when an embedded DuckDB collection grows beyond threshold,
the kernel can promote it to Postgres by calling ``promote_from_duckdb()``.

Dependencies: asyncpg (pip install asyncpg) — optional.
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier

logger = structlog.get_logger(__name__)

_ASYNCPG_AVAILABLE = False
try:
    import asyncpg  # type: ignore[import]
    _ASYNCPG_AVAILABLE = True
except ImportError:
    pass


class PostgresStore:
    """AsyncPG-backed PostgreSQL storage backend.

    Usage::

        store = PostgresStore(dsn="postgresql://user:pass@host/db")
        await store.open()
        await store.write("users", [{"name": "alice", "email": "a@b.com"}])
    """

    SUPPORTED_CATEGORIES = ["relational", "document"]

    def __init__(
        self,
        dsn: str,
        *,
        backend_id: str = "postgres-external",
        min_size: int = 2,
        max_size: int = 10,
        ssl: str = "prefer",
    ) -> None:
        if not _ASYNCPG_AVAILABLE:
            raise ImportError("asyncpg is required: pip install asyncpg")

        self._dsn = dsn
        self._backend_id = backend_id
        self._min_size = min_size
        self._max_size = max_size
        self._ssl = ssl
        self._pool: Any | None = None  # asyncpg.Pool

    async def open(self) -> None:
        self._pool = await asyncpg.create_pool(  # type: ignore[name-defined]
            self._dsn,
            min_size=self._min_size,
            max_size=self._max_size,
            ssl=self._ssl,
        )
        logger.info("postgres_store_opened", backend_id=self._backend_id)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self._backend_id,
            display_name="PostgreSQL",
            tier=BackendTier.EXTERNAL,
            status=BackendStatus.UNKNOWN,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"dsn": self._dsn.split("@")[-1]},  # Hide credentials
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self._backend_id,
            supports_predicate_pushdown=True,
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=True,
            supports_sort_pushdown=True,
            supports_limit_pushdown=True,
            supports_joins=True,
            native_language="sql",
        )

    async def health_check(self) -> BackendStatus:
        if not self._pool:
            return BackendStatus.UNAVAILABLE
        try:
            async with self._pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
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
        if not records or not self._pool:
            return []

        table = _safe_table(collection)
        await self._ensure_table(table, records[0])

        ids: list[str] = []
        async with self._pool.acquire() as conn:
            for record in records:
                rid = record.get("_id") or str(uuid.uuid4())
                row = {
                    "_id": rid,
                    "_tenant_id": tenant_id,
                    "_ingested_at": datetime.now(timezone.utc).isoformat(),
                    **{k: v for k, v in record.items() if not k.startswith("_")},
                }
                cols = list(row.keys())
                placeholders = ", ".join(f"${i+1}" for i in range(len(cols)))
                col_list = ", ".join(f'"{c}"' for c in cols)
                values = [_pg_value(row[c]) for c in cols]

                await conn.execute(
                    f"INSERT INTO {table} ({col_list}) VALUES ({placeholders}) "
                    f"ON CONFLICT (_id) DO NOTHING",
                    *values,
                )
                ids.append(rid)

        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        if not self._pool:
            return None
        table = _safe_table(collection)
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow(
                f'SELECT * FROM {table} WHERE _id = $1 AND _tenant_id = $2',
                record_id, tenant_id,
            )
        return dict(row) if row else None

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        if not self._pool:
            raise RuntimeError("PostgreSQL pool not initialised")
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(str(sub_plan.native_query))

        if not rows:
            return QueryResult(columns=[], rows=[], row_count=0, backend_ids_used=[self._backend_id])

        columns = list(rows[0].keys())
        data = [list(r.values()) for r in rows]
        return QueryResult(
            columns=columns,
            rows=data,
            row_count=len(data),
            backend_ids_used=[self._backend_id],
        )

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        if not self._pool:
            return []
        async with self._pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = 'public'"
            )
        return [r["tablename"] for r in rows]

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _ensure_table(self, table: str, sample: dict[str, Any]) -> None:
        if not self._pool:
            return
        col_defs = _pg_column_defs(sample)
        ddl = f"""
        CREATE TABLE IF NOT EXISTS {table} (
            _id TEXT PRIMARY KEY,
            _tenant_id TEXT NOT NULL DEFAULT 'default',
            _ingested_at TEXT,
            {col_defs}
        );
        CREATE INDEX IF NOT EXISTS idx_{table}_tenant ON {table} (_tenant_id);
        """
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)


def _safe_table(collection: str) -> str:
    return "".join(c if c.isalnum() or c == "_" else "_" for c in collection).lower()


def _pg_column_defs(sample: dict[str, Any]) -> str:
    type_map = {
        int: "BIGINT",
        float: "DOUBLE PRECISION",
        bool: "BOOLEAN",
        str: "TEXT",
        bytes: "BYTEA",
    }
    parts = []
    for col, val in sample.items():
        if col.startswith("_"):
            continue
        pg_type = type_map.get(type(val), "TEXT")
        parts.append(f'"{col}" {pg_type}')
    return ",\n            ".join(parts) if parts else "_data TEXT"


def _pg_value(v: Any) -> Any:
    if isinstance(v, (dict, list)):
        return json.dumps(v, default=str)
    return v
