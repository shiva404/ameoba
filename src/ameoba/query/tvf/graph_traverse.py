"""graph_traverse TVF — multi-hop graph traversal via Neo4j / Cypher.

SQL surface::

    SELECT * FROM graph_traverse(
        collection  := 'knowledge_graph',
        start_id    := 'node-123',
        depth       := 2,
        direction   := 'outbound',  -- 'outbound' | 'inbound' | 'any'
        rel_types   := 'RELATES_TO,DEPENDS_ON'  -- optional CSV
    )

Routes to the first AVAILABLE backend that supports the 'graph' category.
Emits a Cypher query; the result is flattened to tabular rows for federation.
"""

from __future__ import annotations

from typing import Any

import structlog

from ameoba.domain.query import QueryResult, SubPlan
from ameoba.domain.routing import BackendStatus

logger = structlog.get_logger(__name__)

_DIRECTION_MAP = {
    "outbound": "->",
    "inbound": "<-",
    "any": "-",
}


class GraphTraverseTVF:
    name = "graph_traverse"

    async def execute(
        self,
        args: dict[str, Any],
        topology: Any,
    ) -> QueryResult:
        """Execute a multi-hop graph traversal.

        Args:
            args: {
                collection: str,
                start_id:   str,
                depth:      int (default 1, max 5),
                direction:  "outbound" | "inbound" | "any" (default "outbound"),
                rel_types:  str | None (CSV of relationship type names),
                tenant_id:  str (default "default"),
            }
            topology: TopologyRegistry
        """
        collection: str = args["collection"]
        start_id: str = args["start_id"]
        depth: int = min(int(args.get("depth", 1)), 5)  # cap at 5 hops
        direction: str = args.get("direction", "outbound").lower()
        rel_types_raw: str | None = args.get("rel_types")
        tenant_id: str = args.get("tenant_id", "default")

        backend = topology.find_backend("graph")
        if backend is None:
            raise RuntimeError(
                "No graph-capable backend registered. "
                "Register a Neo4j backend first."
            )

        status = await backend.health_check()
        if status == BackendStatus.UNAVAILABLE:
            raise RuntimeError(f"Graph backend unavailable: {backend.descriptor.id}")

        cypher = _build_cypher(collection, start_id, depth, direction, rel_types_raw, tenant_id)
        logger.debug("graph_traverse_cypher", cypher=cypher)

        sub_plan = SubPlan(
            backend_id=backend.descriptor.id,
            collection=collection,
            native_query=cypher,
            limit=None,
        )
        return await backend.execute_sub_plan(sub_plan)


def _build_cypher(
    collection: str,
    start_id: str,
    depth: int,
    direction: str,
    rel_types_raw: str | None,
    tenant_id: str,
) -> str:
    """Build a Cypher MATCH clause for the traversal."""
    label = "".join(c if c.isalnum() else "_" for c in collection).capitalize()

    arrow = _DIRECTION_MAP.get(direction, "->")
    if direction == "outbound":
        rel_pattern = f"-[r*1..{depth}]->"
    elif direction == "inbound":
        rel_pattern = f"<-[r*1..{depth}]-"
    else:
        rel_pattern = f"-[r*1..{depth}]-"

    if rel_types_raw:
        rel_types = [t.strip().upper() for t in rel_types_raw.split(",") if t.strip()]
        if rel_types:
            type_filter = "|".join(rel_types)
            if direction == "outbound":
                rel_pattern = f"-[r:{type_filter}*1..{depth}]->"
            elif direction == "inbound":
                rel_pattern = f"<-[r:{type_filter}*1..{depth}]-"
            else:
                rel_pattern = f"-[r:{type_filter}*1..{depth}]-"

    tenant_filter = ""
    if tenant_id != "default":
        tenant_filter = f" AND start._tenant_id = '{tenant_id}'"

    return (
        f"MATCH (start:{label}){rel_pattern}(end:{label}) "
        f"WHERE start._node_id = '{start_id}'{tenant_filter} "
        f"RETURN start._node_id AS start_id, end._node_id AS end_id, "
        f"type(r[-1]) AS relationship_type, length(r) AS hop_count"
    )
