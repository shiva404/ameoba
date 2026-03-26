"""full_text_search TVF — full-text search via Elasticsearch / OpenSearch.

SQL surface::

    SELECT * FROM full_text_search(
        collection := 'articles',
        query      := 'machine learning embeddings',
        top_k      := 20,
        tenant_id  := 'acme'  -- optional
    )

Routes to the first AVAILABLE backend that supports 'document' category
AND has full_text_search capability (i.e. ElasticsearchStore).
"""

from __future__ import annotations

from typing import Any

import structlog

from ameoba.domain.query import QueryResult
from ameoba.domain.routing import BackendStatus

logger = structlog.get_logger(__name__)


class FullTextSearchTVF:
    name = "full_text_search"

    async def execute(
        self,
        args: dict[str, Any],
        topology: Any,
    ) -> QueryResult:
        """Execute a full-text search via Elasticsearch.

        Args:
            args: {
                collection: str,
                query:      str (the search text),
                top_k:      int (default 20),
                tenant_id:  str (default "default"),
            }
            topology: TopologyRegistry
        """
        collection: str = args["collection"]
        query_text: str = args["query"]
        top_k: int = int(args.get("top_k", 20))
        tenant_id: str = args.get("tenant_id", "default")

        # Find a document backend that supports full-text search
        backend = topology.find_backend("document")
        if backend is None:
            raise RuntimeError(
                "No document-capable backend registered. "
                "Register an Elasticsearch backend first."
            )

        status = await backend.health_check()
        if status == BackendStatus.UNAVAILABLE:
            raise RuntimeError(f"Document backend unavailable: {backend.descriptor.id}")

        if hasattr(backend, "full_text_search"):
            return await backend.full_text_search(
                collection,
                query_text,
                top_k=top_k,
                tenant_id=tenant_id,
            )

        # Fallback: ES DSL match query via execute_sub_plan
        from ameoba.domain.query import SubPlan
        dsl = {
            "query": {
                "bool": {
                    "must": {"multi_match": {"query": query_text, "type": "best_fields"}},
                    "filter": [{"term": {"_tenant_id": tenant_id}}],
                }
            }
        }
        sub_plan = SubPlan(
            backend_id=backend.descriptor.id,
            collection=collection,
            native_query=dsl,
            limit=top_k,
        )
        return await backend.execute_sub_plan(sub_plan)
