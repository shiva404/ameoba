"""Classification pipeline — the central intelligence of Ameoba.

Orchestrates the 4-layer cascade:
  Layer 0 (priority 10):  BinaryBlobDetector  — magic bytes, entropy
  Layer 1 (priority 20):  FormatDetector      — JSON/CSV/XML/Parquet sniffing
  Layer 2 (priority 50):  StructuralAnalyser  — flatness, Jaccard, nesting
  Layer 3 (priority 70):  SemanticClassifier  — graph topology, domain vocab
  Fallback (priority 90): always returns document with low confidence

The pipeline is the sole entry point for classification.  All other code
should call ``pipeline.classify(record)`` — never individual layers.
"""

from __future__ import annotations

import time
from typing import Any

import structlog

from ameoba.config import ClassifierConfig
from ameoba.domain.record import ClassificationVector, DataCategory, DataRecord
from ameoba.kernel.classifier.layers.binary import BinaryBlobDetector
from ameoba.kernel.classifier.layers.format import FormatDetector
from ameoba.kernel.classifier.layers.semantic import SemanticClassifier
from ameoba.kernel.classifier.layers.structural import StructuralAnalyser
from ameoba.kernel.classifier.registry import ClassifierRegistry

logger = structlog.get_logger(__name__)


class _FallbackClassifier:
    """Catch-all: if all other layers returned nothing, assume document."""

    priority: int = 90
    name: str = "fallback"

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        return ClassificationVector(
            document=1.0, confidence=0.1, dominant_layer=self.name
        )


def build_default_registry(cfg: ClassifierConfig | None = None) -> ClassifierRegistry:
    """Build and return a ClassifierRegistry populated with all built-in layers."""
    cfg = cfg or ClassifierConfig()
    registry = ClassifierRegistry()
    registry.register(BinaryBlobDetector(
        entropy_threshold=cfg.blob_entropy_threshold,
        null_byte_threshold=cfg.blob_null_byte_pct_threshold,
    ))
    registry.register(FormatDetector())
    registry.register(StructuralAnalyser(
        relational_jaccard_threshold=cfg.relational_jaccard_threshold,
        document_jaccard_threshold=cfg.document_jaccard_threshold,
        max_relational_nesting_depth=cfg.max_relational_nesting_depth,
    ))
    registry.register(SemanticClassifier())
    registry.register(_FallbackClassifier())
    return registry


class ClassificationPipeline:
    """Entry point for all data classification.

    Usage::

        pipeline = ClassificationPipeline()
        result = pipeline.classify(record)
        # result.primary_category → DataCategory.RELATIONAL
        # result.is_mixed         → True/False (decompose if True)
    """

    def __init__(
        self,
        registry: ClassifierRegistry | None = None,
        cfg: ClassifierConfig | None = None,
    ) -> None:
        self._cfg = cfg or ClassifierConfig()
        self._registry = registry or build_default_registry(self._cfg)

    def classify(self, record: DataRecord) -> ClassificationVector:
        """Classify a DataRecord and return its ClassificationVector.

        If the producer provided a ``category_hint``, it is trusted with
        confidence=1.0 and the cascade is skipped.

        Args:
            record: The DataRecord to classify.

        Returns:
            A normalised ``ClassificationVector``.
        """
        t0 = time.perf_counter()

        # Fast path: explicit hint from producer
        if record.category_hint is not None:
            vec = _hint_to_vector(record.category_hint)
            logger.debug(
                "classification_hint_used",
                record_id=str(record.id),
                category=record.category_hint.value,
            )
            return vec

        # Large payload shortcut: straight to blob
        payload_size = _estimate_size(record.payload)
        if payload_size > self._cfg.direct_blob_size_bytes:
            logger.info(
                "classification_direct_blob",
                record_id=str(record.id),
                size_bytes=payload_size,
            )
            return ClassificationVector(blob=1.0, confidence=1.0, dominant_layer="size_limit")

        context: dict[str, Any] = {
            "content_type": record.content_type or "",
            "collection": record.collection,
            "byte_budget_remaining": self._cfg.streaming_byte_budget,
        }

        result = self._registry.run_cascade(record.payload, context)

        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.debug(
            "classification_complete",
            record_id=str(record.id),
            category=result.primary_category.value,
            confidence=round(result.confidence, 3),
            dominant_layer=result.dominant_layer,
            elapsed_ms=round(elapsed_ms, 2),
        )

        return result

    def classify_batch(self, records: list[DataRecord]) -> list[ClassificationVector]:
        """Classify a batch of records.  Each record is classified independently."""
        return [self.classify(r) for r in records]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _hint_to_vector(category: DataCategory) -> ClassificationVector:
    mapping = {
        DataCategory.RELATIONAL: ClassificationVector(relational=1.0, confidence=1.0, dominant_layer="hint"),
        DataCategory.DOCUMENT:   ClassificationVector(document=1.0,   confidence=1.0, dominant_layer="hint"),
        DataCategory.GRAPH:      ClassificationVector(graph=1.0,      confidence=1.0, dominant_layer="hint"),
        DataCategory.BLOB:       ClassificationVector(blob=1.0,        confidence=1.0, dominant_layer="hint"),
        DataCategory.VECTOR:     ClassificationVector(vector=1.0,      confidence=1.0, dominant_layer="hint"),
        DataCategory.UNKNOWN:    ClassificationVector(document=1.0,    confidence=0.1, dominant_layer="hint"),
    }
    return mapping[category]


def _estimate_size(payload: Any) -> int:
    """Quick size estimate without full serialisation."""
    if isinstance(payload, (bytes, bytearray)):
        return len(payload)
    if isinstance(payload, str):
        return len(payload.encode("utf-8"))
    return 0  # Unknown — do not trigger size limit for structured data
