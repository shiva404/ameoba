"""Backend topology registry.

Tracks all registered storage backends (embedded and external), their
supported categories, and current health status.

The router queries this registry to find the best backend for a given
ClassificationVector.  The staging buffer uses it to detect when backends
become available again.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import structlog

from ameoba.domain.record import DataCategory
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier
from ameoba.ports.storage import StorageBackend

logger = structlog.get_logger(__name__)


class TopologyRegistry:
    """In-memory registry of storage backends.

    Thread-safe (asyncio lock protects all mutations).

    Usage::

        registry = TopologyRegistry()
        registry.register(descriptor, backend)
        backend = registry.find_backend(DataCategory.RELATIONAL)
    """

    def __init__(self) -> None:
        self._lock = asyncio.Lock()
        # descriptor id → (descriptor, backend_instance)
        self._backends: dict[str, tuple[BackendDescriptor, StorageBackend]] = {}

    async def register(
        self,
        descriptor: BackendDescriptor,
        backend: StorageBackend,
    ) -> None:
        """Register a backend.  Replaces any previous registration with the same id."""
        async with self._lock:
            self._backends[descriptor.id] = (descriptor, backend)
        logger.info(
            "backend_registered",
            backend_id=descriptor.id,
            tier=descriptor.tier.value,
            categories=descriptor.supported_categories,
        )

    async def deregister(self, backend_id: str) -> None:
        async with self._lock:
            self._backends.pop(backend_id, None)
        logger.info("backend_deregistered", backend_id=backend_id)

    def get_backend(self, backend_id: str) -> StorageBackend | None:
        """Retrieve a backend instance by id (read-only, no lock needed)."""
        entry = self._backends.get(backend_id)
        return entry[1] if entry else None

    def get_descriptor(self, backend_id: str) -> BackendDescriptor | None:
        entry = self._backends.get(backend_id)
        return entry[0] if entry else None

    def find_backend(
        self,
        category: DataCategory,
        *,
        tier_preference: BackendTier | None = None,
    ) -> tuple[BackendDescriptor, StorageBackend] | None:
        """Find the best available backend for a data category.

        Selection priority:
        1. AVAILABLE status preferred over DEGRADED
        2. If tier_preference is set, prefer that tier
        3. Embedded tier preferred for small workloads (default)

        Returns:
            (descriptor, backend) tuple, or None if no backend available.
        """
        candidates = [
            (desc, be)
            for desc, be in self._backends.values()
            if category.value in desc.supported_categories
            and desc.status in (BackendStatus.AVAILABLE, BackendStatus.DEGRADED)
        ]

        if not candidates:
            return None

        def sort_key(item: tuple[BackendDescriptor, StorageBackend]) -> tuple[int, int]:
            desc = item[0]
            # Lower number = preferred
            status_rank = 0 if desc.status == BackendStatus.AVAILABLE else 1
            tier_rank = 0 if desc.tier == BackendTier.EMBEDDED else 1
            if tier_preference and desc.tier == tier_preference:
                tier_rank = 0
            return (status_rank, tier_rank)

        candidates.sort(key=sort_key)
        return candidates[0]

    async def health_check_all(self) -> dict[str, BackendStatus]:
        """Probe all registered backends and update their status.

        Returns a mapping of backend_id → current status.
        """
        results: dict[str, BackendStatus] = {}

        async def probe(backend_id: str, backend: StorageBackend) -> None:
            try:
                status = await backend.health_check()
            except Exception as exc:
                logger.warning(
                    "backend_health_check_error",
                    backend_id=backend_id,
                    error=str(exc),
                )
                status = BackendStatus.UNAVAILABLE

            results[backend_id] = status

            # Update descriptor in-place
            async with self._lock:
                if backend_id in self._backends:
                    desc, be = self._backends[backend_id]
                    updated = desc.model_copy(update={
                        "status": status,
                        "last_health_check": datetime.now(timezone.utc),
                    })
                    self._backends[backend_id] = (updated, be)

        await asyncio.gather(*(
            probe(bid, be)
            for bid, (_, be) in self._backends.items()
        ))
        return results

    def list_descriptors(self) -> list[BackendDescriptor]:
        return [desc for desc, _ in self._backends.values()]

    def list_backends(self) -> list[tuple[BackendDescriptor, StorageBackend]]:
        return list(self._backends.values())
