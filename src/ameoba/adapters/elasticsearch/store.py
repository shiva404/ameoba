"""Elasticsearch / OpenSearch document storage backend.

Handles document data with full-text search and semi-structured queries.
Also supports kNN vector search for co-located embeddings.

Dependencies: elasticsearch[async] (pip install elasticsearch[async]) — optional.
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

_ES_AVAILABLE = False
try:
    from elasticsearch import AsyncElasticsearch  # type: ignore[import]
    _ES_AVAILABLE = True
except ImportError:
    pass


class ElasticsearchStore:
    """Elasticsearch storage backend for document and kNN vector data.

    Usage::

        store = ElasticsearchStore(hosts=["http://localhost:9200"])
        await store.open()
        await store.write("articles", [{"title": "Hello", "body": "..."}])
    """

    SUPPORTED_CATEGORIES = ["document", "vector"]

    def __init__(
        self,
        hosts: list[str],
        *,
        backend_id: str = "elasticsearch-external",
        index_prefix: str = "ameoba_",
        refresh_on_write: str = "false",  # "true" for tests, "false" for production
    ) -> None:
        if not _ES_AVAILABLE:
            raise ImportError(
                "elasticsearch package is required: pip install elasticsearch[async]"
            )
        self._hosts = hosts
        self._backend_id = backend_id
        self._index_prefix = index_prefix
        self._refresh = refresh_on_write
        self._client: Any = None

    async def open(self) -> None:
        self._client = AsyncElasticsearch(self._hosts)  # type: ignore[name-defined]
        logger.info("elasticsearch_store_opened", backend_id=self._backend_id)

    async def close(self) -> None:
        if self._client:
            await self._client.close()
            self._client = None

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self._backend_id,
            display_name="Elasticsearch",
            tier=BackendTier.EXTERNAL,
            status=BackendStatus.UNKNOWN,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"hosts": self._hosts},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self._backend_id,
            supports_predicate_pushdown=True,    # term filters, range filters
            supports_projection_pushdown=True,   # _source filtering
            supports_aggregation_pushdown=True,  # bucket / metric aggregations
            supports_sort_pushdown=True,
            supports_limit_pushdown=True,
            supports_joins=False,
            native_language="es_dsl",
        )

    async def health_check(self) -> BackendStatus:
        if not self._client:
            return BackendStatus.UNAVAILABLE
        try:
            info = await self._client.cluster.health()
            status = info.get("status", "red")
            if status == "green":
                return BackendStatus.AVAILABLE
            if status == "yellow":
                return BackendStatus.DEGRADED
            return BackendStatus.UNAVAILABLE
        except Exception:
            return BackendStatus.UNAVAILABLE

    async def write(
        self,
        collection: str,
        records: list[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> list[str]:
        if not records or not self._client:
            return []

        index = self._index_name(collection)
        ids: list[str] = []

        for record in records:
            doc_id = record.get("_id") or str(uuid.uuid4())
            doc = {
                "_tenant_id": tenant_id,
                "_ingested_at": datetime.now(timezone.utc).isoformat(),
                **{k: v for k, v in record.items() if k != "_id"},
            }
            await self._client.index(
                index=index,
                id=doc_id,
                document=doc,
                refresh=self._refresh,
            )
            ids.append(doc_id)

        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        if not self._client:
            return None
        try:
            resp = await self._client.get(
                index=self._index_name(collection), id=record_id
            )
            source = resp["_source"]
            if source.get("_tenant_id") != tenant_id and tenant_id != "default":
                return None
            return source
        except Exception:
            return None

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        """Execute an ES DSL query sub-plan."""
        if not self._client:
            raise RuntimeError("Elasticsearch client not initialised")

        query = sub_plan.native_query
        if isinstance(query, str):
            try:
                query = json.loads(query)
            except json.JSONDecodeError:
                query = {"query": {"query_string": {"query": query}}}

        index = self._index_name(sub_plan.collection or "documents")
        limit = sub_plan.limit or 100

        resp = await self._client.search(
            index=index,
            body=query,
            size=limit,
        )

        hits = resp["hits"]["hits"]
        if not hits:
            return QueryResult(columns=[], rows=[], row_count=0, backend_ids_used=[self._backend_id])

        # Flatten _source with _id and _score
        columns = ["_id", "_score"] + list(hits[0]["_source"].keys())
        rows = [
            [h["_id"], h["_score"]] + list(h["_source"].values())
            for h in hits
        ]
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            backend_ids_used=[self._backend_id],
        )

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        if not self._client:
            return []
        resp = await self._client.indices.get(index=f"{self._index_prefix}*")
        prefix = self._index_prefix
        return [name[len(prefix):] for name in resp if name.startswith(prefix)]

    async def full_text_search(
        self,
        collection: str,
        query_text: str,
        *,
        top_k: int = 20,
        tenant_id: str = "default",
    ) -> QueryResult:
        """Full-text search — used by the es.search() TVF."""
        dsl: dict[str, Any] = {
            "query": {
                "bool": {
                    "must": {"multi_match": {"query": query_text, "type": "best_fields"}},
                    "filter": [{"term": {"_tenant_id": tenant_id}}],
                }
            }
        }
        sub_plan = SubPlan(
            backend_id=self._backend_id,
            collection=collection,
            native_query=dsl,
            limit=top_k,
        )
        return await self.execute_sub_plan(sub_plan)

    # ------------------------------------------------------------------

    def _index_name(self, collection: str) -> str:
        safe = "".join(c if c.isalnum() or c == "_" else "_" for c in collection).lower()
        return f"{self._index_prefix}{safe}"
