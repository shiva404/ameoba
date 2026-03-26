"""vector_search TVF — approximate nearest-neighbour search.

SQL surface::

    SELECT * FROM vector_search(
        collection  := 'embeddings',
        query_vector := ARRAY[0.1, 0.2, ...],
        top_k       := 10,
        filter      := 'category = ''docs'''  -- optional
    )

Routes to the first AVAILABLE backend that supports the 'vector' category.
Falls back to DuckDB VSS if no dedicated vector store is registered.
"""

from __future__ import annotations

from typing import Any

import structlog

from ameoba.domain.query import QueryResult
from ameoba.domain.routing import BackendStatus

logger = structlog.get_logger(__name__)


class VectorSearchTVF:
    name = "vector_search"

    async def execute(
        self,
        args: dict[str, Any],
        topology: Any,
    ) -> QueryResult:
        """Execute a vector nearest-neighbour search via the topology.

        Args:
            args: {
                collection:   str,
                query_vector: list[float],
                top_k:        int (default 10),
                filter:       str | None,
                tenant_id:    str (default "default"),
            }
            topology: TopologyRegistry
        """
        collection: str = args["collection"]
        query_vector: list[float] = args["query_vector"]
        top_k: int = int(args.get("top_k", 10))
        filter_expr: str | None = args.get("filter")
        tenant_id: str = args.get("tenant_id", "default")

        # Find a vector-capable backend
        backend = topology.find_backend("vector")
        if backend is None:
            raise RuntimeError(
                "No vector-capable backend registered. "
                "Register a LanceDB or Elasticsearch backend first."
            )

        status = await backend.health_check()
        if status == BackendStatus.UNAVAILABLE:
            raise RuntimeError(f"Vector backend is unavailable: {backend.descriptor.id}")

        # Delegate to the backend's vector_search method if available
        if hasattr(backend, "vector_search"):
            return await backend.vector_search(
                collection,
                query_vector,
                top_k=top_k,
                filter_expr=filter_expr,
                tenant_id=tenant_id,
            )

        # Fallback: use execute_sub_plan with a vector dict
        from ameoba.domain.query import SubPlan
        sub_plan = SubPlan(
            backend_id=backend.descriptor.id,
            collection=collection,
            native_query={"vector": query_vector, "filter": filter_expr},
            limit=top_k,
        )
        return await backend.execute_sub_plan(sub_plan)
