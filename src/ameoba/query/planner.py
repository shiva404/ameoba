"""Two-level query planner.

Decides between:
  - Fast path: single backend — translate SQL to native query and execute directly.
  - Federation path: multi-backend — decompose into sub-plans, execute in parallel,
    join results in DuckDB.

The planner is deliberately conservative: when in doubt, use the federation path.
Correctness > performance.
"""

from __future__ import annotations

import re
from typing import Any

import structlog

from ameoba.domain.query import (
    BackendCapabilityManifest,
    JoinStrategy,
    QueryPathKind,
    QueryPlan,
    SubPlan,
)
from ameoba.kernel.topology import TopologyRegistry

logger = structlog.get_logger(__name__)

# Simple regex patterns for SQL parsing (not a full parser — good enough for planning)
_TABLE_PATTERN = re.compile(
    r"""(?:FROM|JOIN)\s+([a-zA-Z0-9_.`"]+)""",
    re.IGNORECASE,
)
_SELECT_COLS_PATTERN = re.compile(r"SELECT\s+(.*?)\s+FROM", re.IGNORECASE | re.DOTALL)
_LIMIT_PATTERN = re.compile(r"\bLIMIT\s+(\d+)", re.IGNORECASE)
_WHERE_PATTERN = re.compile(r"\bWHERE\b(.+?)(?:\bGROUP\b|\bORDER\b|\bLIMIT\b|$)", re.IGNORECASE | re.DOTALL)


class QueryPlanner:
    """Plans the execution of a SQL query against the registered backends.

    The planner uses a simple heuristic: if all referenced tables map to the
    same backend, use the fast path.  Otherwise, use the federation path.

    For the MVP (embedded DuckDB only), every query uses the fast path.
    """

    def __init__(self, topology: TopologyRegistry) -> None:
        self._topology = topology

    def plan(self, sql: str, *, tenant_id: str = "default") -> QueryPlan:
        """Parse the SQL and produce an execution plan.

        Args:
            sql:       The federated SQL query.
            tenant_id: Used to inject tenant isolation filters.

        Returns:
            A ``QueryPlan`` ready to be passed to the executor.
        """
        sql = sql.strip()
        tables = _extract_tables(sql)
        logger.debug("query_planner_tables", tables=tables, sql=sql[:100])

        # Map table names to backends
        table_backends: dict[str, str] = {}
        for table in tables:
            backend_id = self._resolve_table_backend(table)
            table_backends[table] = backend_id or "unknown"

        unique_backends = set(table_backends.values()) - {"unknown"}

        if len(unique_backends) <= 1:
            return self._fast_path_plan(sql, unique_backends, tenant_id)
        else:
            return self._federation_path_plan(sql, table_backends, tenant_id)

    def _fast_path_plan(
        self, sql: str, backends: set[str], tenant_id: str
    ) -> QueryPlan:
        """Single-backend plan — execute SQL directly."""
        backend_id = next(iter(backends)) if backends else _default_backend_id(self._topology)
        limit = _extract_limit(sql)

        sub_plan = SubPlan(
            backend_id=backend_id,
            collection="",
            native_query=_inject_tenant_filter(sql, tenant_id),
            limit=limit,
        )

        return QueryPlan(
            original_sql=sql,
            path=QueryPathKind.FAST,
            sub_plans=[sub_plan],
            join_strategy=JoinStrategy.HASH_JOIN,
        )

    def _federation_path_plan(
        self,
        sql: str,
        table_backends: dict[str, str],
        tenant_id: str,
    ) -> QueryPlan:
        """Multi-backend plan — decompose and join in DuckDB."""
        # Group tables by backend
        backend_tables: dict[str, list[str]] = {}
        for table, bid in table_backends.items():
            backend_tables.setdefault(bid, []).append(table)

        sub_plans: list[SubPlan] = []
        limit = _extract_limit(sql)

        for backend_id, tables in backend_tables.items():
            # Build a per-backend sub-query (simplified: SELECT * from each table)
            for table in tables:
                sub_sql = f"SELECT * FROM {table}"
                where = _extract_where_clause(sql)
                if where:
                    sub_sql += f" WHERE {where}"
                if limit:
                    sub_sql += f" LIMIT {limit}"

                sub_plans.append(SubPlan(
                    backend_id=backend_id,
                    collection=table,
                    native_query=_inject_tenant_filter(sub_sql, tenant_id),
                    limit=limit,
                ))

        # The federation SQL joins the temp tables in DuckDB
        federation_sql = _build_federation_sql(sql, table_backends)

        # Choose join strategy based on expected row counts
        join_strategy = _choose_join_strategy(sub_plans)

        return QueryPlan(
            original_sql=sql,
            path=QueryPathKind.FEDERATION,
            sub_plans=sub_plans,
            join_strategy=join_strategy,
            federation_sql=federation_sql,
        )

    def _resolve_table_backend(self, table: str) -> str | None:
        """Map a table reference to the backend that owns it.

        Supports two formats:
        - ``backend_prefix.table_name`` — explicit routing (e.g. ``pg.users``)
        - ``table_name`` — inferred from topology (falls back to DuckDB)
        """
        if "." in table:
            prefix, _ = table.split(".", 1)
            # Look for a backend whose id starts with the prefix
            for desc in self._topology.list_descriptors():
                if desc.id.startswith(prefix):
                    return desc.id

        # Unqualified table — assume DuckDB (embedded)
        from ameoba.adapters.embedded.duckdb_store import DuckDBStore
        return DuckDBStore.BACKEND_ID


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _extract_tables(sql: str) -> list[str]:
    """Extract table references from a SQL statement."""
    matches = _TABLE_PATTERN.findall(sql)
    # Normalise: strip quotes and aliases
    tables = []
    for m in matches:
        m = m.strip("`\"'")
        tables.append(m)
    return list(dict.fromkeys(tables))  # deduplicate preserving order


def _extract_limit(sql: str) -> int | None:
    m = _LIMIT_PATTERN.search(sql)
    return int(m.group(1)) if m else None


def _extract_where_clause(sql: str) -> str | None:
    m = _WHERE_PATTERN.search(sql)
    return m.group(1).strip() if m else None


def _inject_tenant_filter(sql: str, tenant_id: str) -> str:
    """Inject a tenant_id filter — simple append approach for the MVP.

    Production note: a proper SQL parser (e.g. sqlglot) should be used
    to correctly inject into any position in the WHERE clause.
    """
    if tenant_id == "default":
        return sql  # No isolation in single-tenant mode
    if "_tenant_id" not in sql:
        # Append as an additional WHERE condition
        if "WHERE" in sql.upper():
            return sql + f" AND _tenant_id = '{tenant_id}'"
        else:
            return sql + f" WHERE _tenant_id = '{tenant_id}'"
    return sql


def _build_federation_sql(sql: str, table_backends: dict[str, str]) -> str:
    """Build the DuckDB SQL that joins the temp tables.

    For now this is the original SQL unchanged — the executor will create
    temp tables from each sub-plan result and DuckDB will join them.
    """
    return sql


def _choose_join_strategy(sub_plans: list[SubPlan]) -> JoinStrategy:
    """Choose a join strategy based on sub-plan characteristics."""
    # Without cost statistics we default to hash join
    return JoinStrategy.HASH_JOIN


def _default_backend_id(topology: TopologyRegistry) -> str:
    """Return the ID of the first available backend (fallback)."""
    from ameoba.adapters.embedded.duckdb_store import DuckDBStore
    descriptors = topology.list_descriptors()
    if descriptors:
        return descriptors[0].id
    return DuckDBStore.BACKEND_ID
