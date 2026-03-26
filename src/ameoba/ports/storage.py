"""StorageBackend protocol — the contract every storage adapter must satisfy."""

from __future__ import annotations

from typing import Any, AsyncIterator, Protocol, runtime_checkable

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus


@runtime_checkable
class StorageBackend(Protocol):
    """Async interface for a storage backend.

    Each backend (DuckDB, Postgres, Neo4j, etc.) implements this protocol.
    The kernel router selects backends based on the classified data category
    and the topology registry; it never calls adapters directly by type.
    """

    @property
    def descriptor(self) -> BackendDescriptor:
        """Immutable description of this backend."""
        ...

    async def health_check(self) -> BackendStatus:
        """Probe the backend and return its current status."""
        ...

    async def write(
        self,
        collection: str,
        records: list[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> list[str]:
        """Write records to the given collection.

        Returns:
            List of backend-assigned record IDs (one per input record).
        """
        ...

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        """Fetch a single record by its backend-assigned ID."""
        ...

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        """Execute a portion of a federated query natively.

        The query planner pushes down predicates, projections, and limits to
        this backend.  The backend executes the ``sub_plan.native_query`` and
        returns typed results.
        """
        ...

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        """Return all collection names visible to the caller."""
        ...

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        """Declare what the backend can push down (used by the query planner)."""
        ...

    async def close(self) -> None:
        """Release resources (connections, file handles, etc.)."""
        ...


@runtime_checkable
class StreamingStorageBackend(StorageBackend, Protocol):
    """Extension for backends that support streaming writes."""

    def write_stream(
        self,
        collection: str,
        records: AsyncIterator[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> AsyncIterator[str]:
        """Stream records in, stream IDs back."""
        ...
