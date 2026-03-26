"""Kernel router — maps ClassificationVector to BackendTarget(s).

The router is the decision point between classification and storage.
It consults the topology registry to find available backends for each
data category, and handles mixed-data decomposition.
"""

from __future__ import annotations

import structlog

from ameoba.domain.record import ClassificationVector, DataCategory, DataRecord
from ameoba.domain.routing import BackendTarget, BackendTier, RoutingDecision
from ameoba.kernel.topology import TopologyRegistry

logger = structlog.get_logger(__name__)

# Records with any category score below this are not routed to that backend
_MIN_CATEGORY_SCORE = 0.2


class KernelRouter:
    """Routes classified DataRecords to the appropriate storage backends.

    Usage::

        router = KernelRouter(topology=registry)
        decision = router.route(record, classification_vector)
    """

    def __init__(self, topology: TopologyRegistry) -> None:
        self._topology = topology

    def route(
        self,
        record: DataRecord,
        vector: ClassificationVector,
    ) -> RoutingDecision:
        """Determine which backends should receive this record.

        For mixed data (``vector.is_mixed``), the record is routed to multiple
        backends (one per significant category).  Cross-references are stored
        in the record metadata.

        Args:
            record: The DataRecord being ingested.
            vector: Its classification result.

        Returns:
            A ``RoutingDecision`` with one or more ``BackendTarget`` entries.
        """
        targets: list[BackendTarget] = []

        if vector.is_mixed:
            targets = self._route_mixed(record, vector)
        else:
            primary = vector.primary_category
            target = self._find_target(primary, record.collection)
            if target:
                targets.append(target)

        if not targets:
            # Fallback: document store (most flexible)
            fallback = self._find_target(DataCategory.DOCUMENT, record.collection)
            if fallback:
                targets.append(fallback)
                logger.warning(
                    "router_fallback_to_document",
                    record_id=str(record.id),
                    primary_category=vector.primary_category.value,
                )

        decision = RoutingDecision(
            record_id=record.id,
            targets=targets,
            classification_summary=(
                f"{vector.primary_category.value} "
                f"(confidence={vector.confidence:.2f}, "
                f"layer={vector.dominant_layer})"
            ),
        )

        logger.debug(
            "routing_decision",
            record_id=str(record.id),
            targets=[t.backend_id for t in targets],
            category=vector.primary_category.value,
            is_mixed=vector.is_mixed,
        )

        return decision

    def _route_mixed(
        self, record: DataRecord, vector: ClassificationVector
    ) -> list[BackendTarget]:
        """Route a mixed record to all backends with significant category scores."""
        targets: list[BackendTarget] = []
        seen_backends: set[str] = set()

        category_scores = [
            (DataCategory.RELATIONAL, vector.relational),
            (DataCategory.DOCUMENT, vector.document),
            (DataCategory.GRAPH, vector.graph),
            (DataCategory.BLOB, vector.blob),
            (DataCategory.VECTOR, vector.vector),
        ]

        for category, score in sorted(category_scores, key=lambda x: x[1], reverse=True):
            if score < _MIN_CATEGORY_SCORE:
                continue
            target = self._find_target(category, record.collection)
            if target and target.backend_id not in seen_backends:
                # Mark only the first (highest-score) target as primary
                is_primary = len(targets) == 0
                targets.append(target.model_copy(update={"is_primary": is_primary}))
                seen_backends.add(target.backend_id)

        return targets

    def _find_target(
        self, category: DataCategory, collection: str
    ) -> BackendTarget | None:
        """Locate a backend for the given category and return a BackendTarget."""
        result = self._topology.find_backend(category)
        if result is None:
            logger.warning(
                "router_no_backend_for_category",
                category=category.value,
            )
            return None

        desc, _ = result
        return BackendTarget(
            backend_id=desc.id,
            collection=collection,
            tier=desc.tier,
        )
