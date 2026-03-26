"""Neo4j graph storage backend.

Uses the official neo4j Python driver with async support.
Writes graph data as nodes + relationships using Cypher.

Dependencies: neo4j (pip install neo4j) — optional.
"""

from __future__ import annotations

import uuid
from typing import Any

import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier

logger = structlog.get_logger(__name__)

_NEO4J_AVAILABLE = False
try:
    from neo4j import AsyncGraphDatabase  # type: ignore[import]
    _NEO4J_AVAILABLE = True
except ImportError:
    pass


class Neo4jStore:
    """Neo4j storage backend for graph data.

    Writes graph records as labelled property graph nodes and edges.
    If the record has ``nodes`` and ``edges`` keys, both are written.
    Otherwise the whole record is written as a single node.

    Usage::

        store = Neo4jStore(uri="bolt://localhost:7687", user="neo4j", password="pass")
        await store.open()
        await store.write("knowledge_graph", [
            {"nodes": [...], "edges": [...]}
        ])
    """

    SUPPORTED_CATEGORIES = ["graph"]

    def __init__(
        self,
        uri: str,
        *,
        user: str = "neo4j",
        password: str = "neo4j",
        backend_id: str = "neo4j-external",
        database: str = "neo4j",
    ) -> None:
        if not _NEO4J_AVAILABLE:
            raise ImportError("neo4j driver is required: pip install neo4j")
        self._uri = uri
        self._user = user
        self._password = password
        self._backend_id = backend_id
        self._database = database
        self._driver: Any = None

    async def open(self) -> None:
        self._driver = AsyncGraphDatabase.driver(  # type: ignore[name-defined]
            self._uri, auth=(self._user, self._password)
        )
        await self._driver.verify_connectivity()
        logger.info("neo4j_store_opened", backend_id=self._backend_id)

    async def close(self) -> None:
        if self._driver:
            await self._driver.close()
            self._driver = None

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self._backend_id,
            display_name="Neo4j",
            tier=BackendTier.EXTERNAL,
            status=BackendStatus.UNKNOWN,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"uri": self._uri, "database": self._database},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self._backend_id,
            supports_predicate_pushdown=True,   # Cypher property filters
            supports_projection_pushdown=True,
            supports_aggregation_pushdown=True,  # count, collect
            supports_sort_pushdown=True,
            supports_limit_pushdown=True,
            supports_joins=False,               # No cross-collection joins in Cypher
            native_language="cypher",
        )

    async def health_check(self) -> BackendStatus:
        if not self._driver:
            return BackendStatus.UNAVAILABLE
        try:
            async with self._driver.session(database=self._database) as session:
                await session.run("RETURN 1")
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
        if not records or not self._driver:
            return []

        ids: list[str] = []
        async with self._driver.session(database=self._database) as session:
            for record in records:
                rid = str(uuid.uuid4())
                if "nodes" in record and "edges" in record:
                    await _write_graph(session, collection, record, rid, tenant_id)
                else:
                    await _write_node(session, collection, record, rid, tenant_id)
                ids.append(rid)

        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        if not self._driver:
            return None
        cypher = (
            f"MATCH (n:{_label(collection)}) "
            "WHERE n._id = $id AND n._tenant_id = $tenant "
            "RETURN n LIMIT 1"
        )
        async with self._driver.session(database=self._database) as session:
            result = await session.run(cypher, id=record_id, tenant=tenant_id)
            record = await result.single()
        return dict(record["n"]) if record else None

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        """Execute a Cypher sub-plan."""
        if not self._driver:
            raise RuntimeError("Neo4j driver not initialised")

        cypher = str(sub_plan.native_query)
        async with self._driver.session(database=self._database) as session:
            result = await session.run(cypher)
            records = await result.data()

        if not records:
            return QueryResult(columns=[], rows=[], row_count=0, backend_ids_used=[self._backend_id])

        columns = list(records[0].keys())
        rows = [[r.get(c) for c in columns] for r in records]
        return QueryResult(
            columns=columns,
            rows=rows,
            row_count=len(rows),
            backend_ids_used=[self._backend_id],
        )

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        if not self._driver:
            return []
        async with self._driver.session(database=self._database) as session:
            result = await session.run("CALL db.labels() YIELD label RETURN label")
            records = await result.data()
        return [r["label"] for r in records]


# ---------------------------------------------------------------------------
# Cypher helpers
# ---------------------------------------------------------------------------

def _label(collection: str) -> str:
    """Convert a collection name to a valid Neo4j label."""
    return "".join(c if c.isalnum() else "_" for c in collection).capitalize()


async def _write_node(session: Any, collection: str, record: dict, rid: str, tenant_id: str) -> None:
    label = _label(collection)
    props = {
        "_id": rid,
        "_tenant_id": tenant_id,
        **{k: v for k, v in record.items() if isinstance(v, (str, int, float, bool))},
    }
    cypher = f"CREATE (n:{label} $props)"
    await session.run(cypher, props=props)


async def _write_graph(session: Any, collection: str, record: dict, rid: str, tenant_id: str) -> None:
    label = _label(collection)
    for node in record.get("nodes", []):
        nid = str(node.get("id", uuid.uuid4()))
        props = {"_graph_id": rid, "_tenant_id": tenant_id, **{
            k: v for k, v in node.items() if isinstance(v, (str, int, float, bool))
        }}
        await session.run(f"MERGE (n:{label} {{_node_id: $nid}}) SET n += $props",
                         nid=nid, props=props)

    for edge in record.get("edges", []):
        src = str(edge.get("source") or edge.get("from") or "")
        tgt = str(edge.get("target") or edge.get("to") or "")
        rel_type = str(edge.get("type") or edge.get("label") or "RELATES_TO").upper()
        if src and tgt:
            cypher = (
                f"MATCH (a:{label} {{_node_id: $src}}), (b:{label} {{_node_id: $tgt}}) "
                f"MERGE (a)-[r:{rel_type}]->(b)"
            )
            await session.run(cypher, src=src, tgt=tgt)
