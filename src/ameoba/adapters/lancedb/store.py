"""LanceDB vector storage backend.

LanceDB provides Apache Arrow-native vector storage with:
- Zero-copy integration with DuckDB (Arrow IPC)
- S3-backed storage (scales to 100M+ vectors without infrastructure)
- ANN search (IVF-PQ, HNSW) at 1.5M IOPS
- INT8 scalar quantization by default (4x compression, <1% recall loss)

Tiering:
- Tier 0 (< 1M vectors): DuckDB VSS extension
- Tier 1 (< 100M vectors): LanceDB ← this adapter
- Tier 2 (≥ 100M vectors): Qdrant / Milvus (see architecture doc)

Dependencies: lancedb (pip install lancedb) — optional.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier

logger = structlog.get_logger(__name__)

_LANCEDB_AVAILABLE = False
try:
    import lancedb  # type: ignore[import]
    import pyarrow as pa  # type: ignore[import]
    _LANCEDB_AVAILABLE = True
except ImportError:
    pass


class LanceDBStore:
    """LanceDB vector storage backend.

    Handles VECTOR data category.  Each collection is a LanceDB table.
    Vectors are stored as fixed-size list columns; metadata is stored
    as additional Arrow columns.

    Usage::

        store = LanceDBStore(uri="./vectors")
        await store.open()
        await store.write("embeddings", [
            {"id": "doc-1", "embedding": [0.1, 0.2, ...], "text": "hello"},
        ])
        results = await store.vector_search("embeddings", query_vec, top_k=10)
    """

    SUPPORTED_CATEGORIES = ["vector"]

    def __init__(
        self,
        uri: str,
        *,
        backend_id: str = "lancedb-embedded",
        vector_field: str = "embedding",
        metric: str = "cosine",
    ) -> None:
        self._uri = uri
        self._backend_id = backend_id
        self._vector_field = vector_field
        self._metric = metric
        self._db: Any = None

    async def open(self) -> None:
        if not _LANCEDB_AVAILABLE:
            raise ImportError("lancedb is required: pip install lancedb pyarrow")
        self._db = await lancedb.connect_async(self._uri)  # type: ignore[name-defined]
        logger.info("lancedb_store_opened", uri=self._uri, backend_id=self._backend_id)

    async def close(self) -> None:
        self._db = None

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self._backend_id,
            display_name="LanceDB (vector)",
            tier=BackendTier.EMBEDDED,  # Can be embedded or S3-backed
            status=BackendStatus.UNKNOWN,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"uri": self._uri, "metric": self._metric},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self._backend_id,
            supports_predicate_pushdown=True,   # Metadata filters
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=False,
            supports_sort_pushdown=False,
            supports_limit_pushdown=True,
            supports_joins=False,
            native_language="none",
        )

    async def health_check(self) -> BackendStatus:
        if not self._db:
            return BackendStatus.UNAVAILABLE
        try:
            await self._db.table_names()
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
        if not records or not self._db:
            return []

        # Detect vector field
        vector_field = self._vector_field
        sample = records[0]
        if vector_field not in sample:
            # Find any field with an embedding-shaped value
            for k, v in sample.items():
                if isinstance(v, list) and len(v) > 10 and all(
                    isinstance(x, (int, float)) for x in v[:4]
                ):
                    vector_field = k
                    break

        ids: list[str] = []
        enriched: list[dict[str, Any]] = []
        for rec in records:
            rid = rec.get("id") or rec.get("_id") or str(uuid.uuid4())
            enriched.append({"id": rid, "_tenant_id": tenant_id, **rec})
            ids.append(rid)

        try:
            table_names = await self._db.table_names()
            if collection in table_names:
                tbl = await self._db.open_table(collection)
                await tbl.add(enriched)
            else:
                await self._db.create_table(collection, data=enriched)
        except Exception as exc:
            logger.error("lancedb_write_error", error=str(exc), collection=collection)
            raise

        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        if not self._db:
            return None
        try:
            tbl = await self._db.open_table(collection)
            results = await tbl.search().where(f"id = '{record_id}'").limit(1).to_list()
            return results[0] if results else None
        except Exception:
            return None

    async def vector_search(
        self,
        collection: str,
        query_vector: list[float],
        *,
        top_k: int = 10,
        filter_expr: str | None = None,
        tenant_id: str = "default",
    ) -> QueryResult:
        """Perform approximate nearest-neighbour search.

        Args:
            collection:   LanceDB table name.
            query_vector: The query embedding.
            top_k:        Number of results to return.
            filter_expr:  Optional metadata filter (e.g. "category = 'docs'").
            tenant_id:    Tenant isolation filter.

        Returns:
            QueryResult with columns including ``_distance``.
        """
        if not self._db:
            raise RuntimeError("LanceDB not opened")

        tbl = await self._db.open_table(collection)
        search = tbl.search(query_vector, vector_column_name=self._vector_field)

        if filter_expr:
            full_filter = filter_expr
            if tenant_id != "default":
                full_filter = f"({filter_expr}) AND _tenant_id = '{tenant_id}'"
            search = search.where(full_filter)
        elif tenant_id != "default":
            search = search.where(f"_tenant_id = '{tenant_id}'")

        results = await search.limit(top_k).to_list()

        if not results:
            return QueryResult(columns=[], rows=[], row_count=0, backend_ids_used=[self._backend_id])

        columns = list(results[0].keys())
        rows = [[r.get(c) for c in columns] for r in results]
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            backend_ids_used=[self._backend_id],
        )

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        """Execute a vector search sub-plan."""
        query = sub_plan.native_query
        if isinstance(query, dict) and "vector" in query:
            return await self.vector_search(
                sub_plan.collection,
                query["vector"],
                top_k=sub_plan.limit or 10,
                filter_expr=query.get("filter"),
            )
        raise NotImplementedError("LanceDB requires a vector query sub-plan")

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        if not self._db:
            return []
        return list(await self._db.table_names())
