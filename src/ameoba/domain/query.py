"""Query planning domain models."""

from __future__ import annotations

from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class JoinStrategy(str, Enum):
    """How the federation engine joins results from different backends."""
    BATCHED_NESTED_LOOP = "batched_nested_loop"  # outer < 100 rows, inner indexed
    HASH_JOIN = "hash_join"                       # default — both sides in DuckDB temp tables
    SEMI_JOIN_BLOOM = "semi_join_bloom"           # one side much larger, Bloom filter pushdown


class QueryPathKind(str, Enum):
    FAST = "fast"           # Single backend — translate directly to native query
    FEDERATION = "federation"  # Multi-backend — decompose, execute, join in DuckDB


class BackendCapabilityManifest(BaseModel):
    """Declares what operations a backend can execute natively (for pushdown)."""

    backend_id: str
    supports_predicate_pushdown: bool = True
    supports_projection_pushdown: bool = True
    supports_aggregation_pushdown: bool = False
    supports_sort_pushdown: bool = False
    supports_limit_pushdown: bool = True
    supports_joins: bool = False

    # Native query language (used for fast-path translation)
    native_language: str = Field(
        default="sql",
        description="sql | cypher | es_dsl | none",
    )


class SubPlan(BaseModel):
    """A fragment of a federated query targeting a single backend."""

    backend_id: str
    collection: str
    native_query: str | dict[str, Any]  # SQL string or ES DSL dict
    projections: list[str] = Field(default_factory=list)  # columns/fields to fetch
    predicates: list[str] = Field(default_factory=list)   # pushed-down filters
    limit: int | None = None
    # Arrow schema expected from this backend (for DuckDB temp table creation)
    expected_columns: list[str] = Field(default_factory=list)


class QueryPlan(BaseModel):
    """The full execution plan for a federated query."""

    original_sql: str
    path: QueryPathKind
    sub_plans: list[SubPlan] = Field(default_factory=list)
    join_strategy: JoinStrategy = JoinStrategy.HASH_JOIN
    join_keys: list[str] = Field(default_factory=list)

    # For federation path: the DuckDB SQL that joins temp tables
    federation_sql: str | None = None

    # Estimated costs (used by optimizer)
    estimated_rows: int | None = None


class QueryResult(BaseModel):
    """Result of executing a QueryPlan."""

    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    backend_ids_used: list[str] = Field(default_factory=list)
    execution_ms: float = 0.0
    truncated: bool = False   # True if result was limited by a row cap
