"""Classifier plugin registry.

Manages a priority-ordered list of ClassifierPlugin implementations.
Same-priority classifiers run in parallel and their results are merged
via calibrated weighted soft voting.

Priority convention:
    10  — BinaryBlobDetector (Layer 0)
    20  — FormatDetector (Layer 1)
    50  — StructuralAnalyser (Layer 2)
    70  — SemanticClassifier (Layer 3)
    90  — FallbackClassifier (always document if nothing else fires)

Custom domain plugins should use priority 30 (between format and structural).
"""

from __future__ import annotations

from typing import Any

import structlog

from ameoba.domain.record import ClassificationVector
from ameoba.ports.classifier import ClassifierPlugin

logger = structlog.get_logger(__name__)


class ClassifierRegistry:
    """Holds registered classifier plugins and runs them in priority order.

    Usage::

        registry = ClassifierRegistry()
        registry.register(BinaryBlobDetector())
        registry.register(StructuralAnalyser())
        vector = registry.run_cascade(data, context={})
    """

    def __init__(self) -> None:
        self._plugins: list[ClassifierPlugin] = []

    def register(self, plugin: ClassifierPlugin) -> None:
        """Register a plugin.  Duplicate names replace the previous entry."""
        self._plugins = [p for p in self._plugins if p.name != plugin.name]
        self._plugins.append(plugin)
        self._plugins.sort(key=lambda p: p.priority)
        logger.debug("classifier_registered", name=plugin.name, priority=plugin.priority)

    def unregister(self, name: str) -> None:
        self._plugins = [p for p in self._plugins if p.name != name]

    def list_plugins(self) -> list[tuple[int, str]]:
        """Return (priority, name) for each registered plugin."""
        return [(p.priority, p.name) for p in self._plugins]

    def run_cascade(
        self,
        data: Any,
        context: dict[str, Any],
        *,
        early_exit_confidence: float = 0.95,
    ) -> ClassificationVector:
        """Run classifiers in priority groups, merging results.

        Algorithm:
        1. Group plugins by priority tier.
        2. Within each tier, run all plugins; merge via weighted soft voting.
        3. If the merged confidence exceeds ``early_exit_confidence``, stop.
        4. Accumulate all tier results by weighted average.

        Args:
            data:                   The raw payload to classify.
            context:                Shared context dict (modified in-place by plugins).
            early_exit_confidence:  Stop the cascade if a tier reaches this confidence.

        Returns:
            A normalised ``ClassificationVector`` representing the best classification.
        """
        if not self._plugins:
            logger.warning("classifier_registry_empty")
            return ClassificationVector(document=1.0, confidence=0.1, dominant_layer="fallback")

        # Group by priority
        priority_groups: dict[int, list[ClassifierPlugin]] = {}
        for plugin in self._plugins:
            priority_groups.setdefault(plugin.priority, []).append(plugin)

        accumulated: list[tuple[ClassificationVector, float]] = []

        for priority in sorted(priority_groups):
            plugins = priority_groups[priority]
            tier_results: list[ClassificationVector] = []

            for plugin in plugins:
                try:
                    result = plugin.classify(data, context)
                    if result is not None:
                        tier_results.append(result)
                except Exception:
                    logger.exception(
                        "classifier_plugin_error",
                        plugin_name=plugin.name,
                        priority=priority,
                    )

            if not tier_results:
                continue

            merged = _soft_vote(tier_results)
            # Store current confidence for layer 3 to inspect
            context["current_confidence"] = merged.confidence
            accumulated.append((merged, merged.confidence))

            if merged.confidence >= early_exit_confidence:
                logger.debug(
                    "classifier_early_exit",
                    priority=priority,
                    confidence=merged.confidence,
                    category=merged.primary_category.value,
                )
                return merged

        if not accumulated:
            return ClassificationVector(document=1.0, confidence=0.1, dominant_layer="fallback")

        # Weight later tiers more (they have more context) but not overwhelmingly
        final = _weighted_accumulate(accumulated)
        return final


def _soft_vote(results: list[ClassificationVector]) -> ClassificationVector:
    """Merge multiple vectors via weighted soft voting (weight = confidence)."""
    if len(results) == 1:
        return results[0]

    total_weight = sum(r.confidence for r in results)
    if total_weight == 0:
        # All zero confidence — plain average
        n = len(results)
        return ClassificationVector(
            relational=sum(r.relational for r in results) / n,
            document=sum(r.document for r in results) / n,
            graph=sum(r.graph for r in results) / n,
            blob=sum(r.blob for r in results) / n,
            vector=sum(r.vector for r in results) / n,
            confidence=0.0,
            dominant_layer=results[0].dominant_layer,
        )

    weights = [r.confidence / total_weight for r in results]
    return ClassificationVector(
        relational=sum(r.relational * w for r, w in zip(results, weights)),
        document=sum(r.document * w for r, w in zip(results, weights)),
        graph=sum(r.graph * w for r, w in zip(results, weights)),
        blob=sum(r.blob * w for r, w in zip(results, weights)),
        vector=sum(r.vector * w for r, w in zip(results, weights)),
        confidence=max(r.confidence for r in results),
        dominant_layer=max(results, key=lambda r: r.confidence).dominant_layer,
    )


def _weighted_accumulate(
    tiers: list[tuple[ClassificationVector, float]],
) -> ClassificationVector:
    """Combine results across tiers; later tiers (higher index) get slightly more weight."""
    if not tiers:
        return ClassificationVector(document=1.0, confidence=0.1, dominant_layer="fallback")

    # Weight by tier position (later = more context) × confidence
    n = len(tiers)
    positional_weights = [(i + 1) / n for i in range(n)]
    combined_weights = [conf * pw for (_, conf), pw in zip(tiers, positional_weights)]
    total = sum(combined_weights)

    if total == 0:
        return tiers[-1][0]

    norm = [w / total for w in combined_weights]
    vecs = [v for v, _ in tiers]

    return ClassificationVector(
        relational=sum(v.relational * w for v, w in zip(vecs, norm)),
        document=sum(v.document * w for v, w in zip(vecs, norm)),
        graph=sum(v.graph * w for v, w in zip(vecs, norm)),
        blob=sum(v.blob * w for v, w in zip(vecs, norm)),
        vector=sum(v.vector * w for v, w in zip(vecs, norm)),
        confidence=max(v.confidence for v in vecs),
        dominant_layer=max(vecs, key=lambda v: v.confidence).dominant_layer,
    )
