"""Query executor — dispatches sub-plans and materialises federation joins.

For the fast path (single backend), the executor translates the plan's
SQL to the backend's native query and returns results directly.

For the federation path (multi-backend):
1. Execute all sub-plans concurrently.
2. Load results into DuckDB in-memory temp tables.
3. Execute the federation SQL (the original JOIN) against the temp tables.
4. Return the merged result set.
"""

from __future__ import annotations

import asyncio
import time
import uuid
from typing import Any

import structlog

from ameoba.domain.query import QueryPathKind, QueryPlan, QueryResult, SubPlan
from ameoba.kernel.topology import TopologyRegistry
from ameoba.ports.storage import StorageBackend

logger = structlog.get_logger(__name__)

# Maximum rows fetched from any single backend sub-plan before federation join
_MAX_ROWS_PER_SUBPLAN = 500_000


class QueryExecutor:
    """Executes a QueryPlan against the topology's registered backends.

    Usage::

        executor = QueryExecutor(topology=topology)
        result = await executor.execute(plan)
    """

    def __init__(self, topology: TopologyRegistry) -> None:
        self._topology = topology

    async def execute(self, plan: QueryPlan) -> QueryResult:
        """Execute a plan and return the result."""
        t0 = time.perf_counter()

        if plan.path == QueryPathKind.FAST:
            result = await self._execute_fast(plan)
        else:
            result = await self._execute_federation(plan)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            "query_executed",
            path=plan.path.value,
            row_count=result.row_count,
            backends=result.backend_ids_used,
            elapsed_ms=round(elapsed_ms, 2),
        )
        return result

    async def _execute_fast(self, plan: QueryPlan) -> QueryResult:
        """Execute directly against a single backend."""
        if not plan.sub_plans:
            return QueryResult(columns=[], rows=[], row_count=0)

        sub_plan = plan.sub_plans[0]
        backend = self._get_backend(sub_plan.backend_id)
        if backend is None:
            raise RuntimeError(f"Backend not found: {sub_plan.backend_id}")

        return await backend.execute_sub_plan(sub_plan)

    async def _execute_federation(self, plan: QueryPlan) -> QueryResult:
        """Execute sub-plans concurrently, then join in DuckDB."""
        if not plan.sub_plans:
            return QueryResult(columns=[], rows=[], row_count=0)

        # 1. Execute all sub-plans concurrently
        sub_results = await asyncio.gather(
            *[self._execute_sub_plan(sp) for sp in plan.sub_plans],
            return_exceptions=True,
        )

        # Fail fast if any join-required sub-plan failed
        for i, r in enumerate(sub_results):
            if isinstance(r, Exception):
                backend_id = plan.sub_plans[i].backend_id
                raise RuntimeError(
                    f"Sub-plan for backend '{backend_id}' failed: {r}"
                ) from r

        # 2. Load into DuckDB in-memory and join
        typed_results: list[QueryResult] = [r for r in sub_results if isinstance(r, QueryResult)]
        return await self._federate_in_duckdb(plan, typed_results)

    async def _execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        backend = self._get_backend(sub_plan.backend_id)
        if backend is None:
            raise RuntimeError(f"Backend not found: {sub_plan.backend_id}")
        return await backend.execute_sub_plan(sub_plan)

    async def _federate_in_duckdb(
        self,
        plan: QueryPlan,
        sub_results: list[QueryResult],
    ) -> QueryResult:
        """Load sub-results into DuckDB temp tables and execute the federation SQL."""
        from ameoba.adapters.embedded.duckdb_store import DuckDBStore

        # Find the DuckDB backend to use as compute engine
        backend = self._get_backend(DuckDBStore.BACKEND_ID)
        if backend is None or not hasattr(backend, "_conn"):
            raise RuntimeError("DuckDB backend not available for federation")

        duckdb_backend: DuckDBStore = backend  # type: ignore[assignment]

        # Create temp tables for each sub-result
        temp_table_map: dict[str, str] = {}  # collection → temp_table_name

        for i, (sub_plan, result) in enumerate(zip(plan.sub_plans, sub_results)):
            temp_name = f"_fed_tmp_{uuid.uuid4().hex[:8]}"
            await _create_temp_table(duckdb_backend, temp_name, result)
            temp_table_map[sub_plan.collection] = temp_name

        # Rewrite federation SQL to use temp table names
        federation_sql = plan.federation_sql or plan.original_sql
        for original_name, temp_name in temp_table_map.items():
            federation_sql = federation_sql.replace(original_name, temp_name)

        result = await duckdb_backend.execute_sql(federation_sql)
        result = result.model_copy(update={
            "backend_ids_used": [sp.backend_id for sp in plan.sub_plans],
        })

        # Cleanup temp tables
        for temp_name in temp_table_map.values():
            try:
                await duckdb_backend._run(f"DROP TABLE IF EXISTS {temp_name}")
            except Exception:
                pass  # Best-effort cleanup

        return result

    def _get_backend(self, backend_id: str) -> StorageBackend | None:
        return self._topology.get_backend(backend_id)


async def _create_temp_table(
    duckdb_backend: Any,
    temp_name: str,
    result: QueryResult,
) -> None:
    """Create a DuckDB temp table from a QueryResult."""
    if not result.rows or not result.columns:
        # Create an empty table
        await duckdb_backend._run(
            f"CREATE TEMP TABLE {temp_name} AS SELECT * FROM (VALUES (NULL)) t(x) WHERE 1=0"
        )
        return

    # Build VALUES clause
    col_defs = ", ".join([f'"{c}" VARCHAR' for c in result.columns])
    await duckdb_backend._run(
        f"CREATE TEMP TABLE {temp_name} ({col_defs})"
    )

    placeholders = ", ".join(["?" for _ in result.columns])
    insert_sql = f"INSERT INTO {temp_name} VALUES ({placeholders})"

    import duckdb
    conn = duckdb_backend._conn

    def _insert() -> None:
        for row in result.rows:
            conn.execute(insert_sql, [str(v) if v is not None else None for v in row])

    import asyncio
    async with duckdb_backend._lock:
        await asyncio.get_event_loop().run_in_executor(None, _insert)
