"""TVF (Table-Valued Function) registry.

TVFs extend Ameoba's federated SQL with backend-native operations that cannot
be expressed as standard SQL:

    vector_search(collection, query_vector, top_k)
    full_text_search(collection, query_text, top_k)
    graph_traverse(start_id, depth, direction)

They are called from the QueryPlanner by rewriting SELECT ... FROM tvf(...)
into a sub-plan against the appropriate backend, loading results into a
temporary DuckDB table, and continuing federation from there.

Registration example::

    registry = TVFRegistry()
    registry.register(VectorSearchTVF())
    handler = registry.resolve("vector_search")
    result = await handler.execute({"collection": "...", "query_vector": [...], "top_k": 5}, topology)
"""

from __future__ import annotations

from typing import Any, Protocol


class TVFHandler(Protocol):
    """Protocol that all TVF implementations must satisfy."""

    name: str  # SQL function name (lower-case)

    async def execute(
        self,
        args: dict[str, Any],
        topology: Any,
    ) -> Any:  # QueryResult
        ...


class TVFRegistry:
    """Registry of available table-valued functions.

    TVFs are registered by name and resolved by the planner.
    """

    def __init__(self) -> None:
        self._handlers: dict[str, TVFHandler] = {}

    def register(self, handler: TVFHandler) -> None:
        self._handlers[handler.name] = handler

    def resolve(self, name: str) -> TVFHandler | None:
        return self._handlers.get(name.lower())

    def names(self) -> list[str]:
        return list(self._handlers.keys())


def build_default_registry() -> TVFRegistry:
    """Build a TVFRegistry with all built-in TVF handlers registered."""
    from ameoba.query.tvf.full_text import FullTextSearchTVF
    from ameoba.query.tvf.graph_traverse import GraphTraverseTVF
    from ameoba.query.tvf.vector_search import VectorSearchTVF

    registry = TVFRegistry()
    registry.register(VectorSearchTVF())
    registry.register(FullTextSearchTVF())
    registry.register(GraphTraverseTVF())
    return registry
