"""Layer 2: Structural analysis.

Inspects the *shape* of parsed data (flatness, key consistency, nesting depth,
type homogeneity) to produce a classification vector.

All heuristic weights are documented inline.  Do not modify thresholds here
without updating the architecture document.
"""

from __future__ import annotations

from typing import Any

from ameoba.domain.record import ClassificationVector
from ameoba.kernel.classifier.heuristics import (
    flatness_ratio,
    graph_signal_count,
    is_likely_embedding,
    jaccard_key_similarity,
    max_nesting_depth,
    tabular_score,
)


class StructuralAnalyser:
    """Layer 2 classifier — inspects data structure for relational/document/graph signals.

    Priority: 50 (runs after binary and format layers).

    Produces a probability distribution, not a single label.  The pipeline
    aggregates this with higher-priority layers if they ran.
    """

    priority: int = 50
    name: str = "structural_analyser"

    def __init__(
        self,
        relational_jaccard_threshold: float = 0.85,
        document_jaccard_threshold: float = 0.5,
        max_relational_nesting_depth: int = 2,
        sample_limit: int = 50,
    ) -> None:
        self._relational_jaccard = relational_jaccard_threshold
        self._document_jaccard = document_jaccard_threshold
        self._max_relational_depth = max_relational_nesting_depth
        self._sample_limit = sample_limit

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        """Analyse the structure of decoded data.

        Uses ``context["decoded"]`` if available (set by the format layer).
        """
        decoded = context.get("decoded", data)

        if decoded is None:
            return None

        # Normalise to a list of records for analysis
        if isinstance(decoded, dict):
            records = [decoded]
        elif isinstance(decoded, list):
            records = decoded[: self._sample_limit]
        else:
            return None  # Scalar or string — not structured

        if not records:
            return None

        return self._classify_records(records)

    def _classify_records(self, records: list[Any]) -> ClassificationVector:
        dict_records = [r for r in records if isinstance(r, dict)]
        non_dict_count = len(records) - len(dict_records)

        # --- Vector detection (check before structural analysis) ---
        vector_score = self._vector_score(dict_records, records)
        if vector_score > 0.7:
            return ClassificationVector(
                vector=vector_score,
                confidence=vector_score,
                dominant_layer=self.name,
            )

        # --- Graph detection ---
        graph_score = self._graph_score(dict_records, records)

        # --- Relational detection ---
        if dict_records:
            tab = tabular_score(dict_records)
            jaccard = jaccard_key_similarity(dict_records)
            flat = flatness_ratio(dict_records)
        else:
            tab = jaccard = flat = 0.0

        # --- Document detection ---
        if dict_records:
            depth = max(max_nesting_depth(r) for r in dict_records)
            schema_variance = 1.0 - jaccard
        else:
            depth = 0
            schema_variance = 1.0

        relational_score = _relational_score(tab, jaccard, flat, depth, self._max_relational_depth)
        document_score = _document_score(schema_variance, depth, self._document_jaccard)

        # Normalise all scores
        total = relational_score + document_score + graph_score
        if total == 0:
            return ClassificationVector(
                document=1.0, confidence=0.3, dominant_layer=self.name
            )

        dominant = max(
            ("relational", relational_score),
            ("document", document_score),
            ("graph", graph_score),
            key=lambda x: x[1],
        )

        return ClassificationVector(
            relational=relational_score / total,
            document=document_score / total,
            graph=graph_score / total,
            confidence=dominant[1] / total,
            dominant_layer=self.name,
        )

    def _graph_score(self, dict_records: list[dict], records: list[Any]) -> float:
        """Sum graph signals across all records; require ≥2 to avoid FK false positives."""
        if not dict_records and not records:
            return 0.0

        total_signals = 0
        for r in records[: self._sample_limit]:
            total_signals += graph_signal_count(r)

        avg_signals = total_signals / len(records) if records else 0

        # Require at least 2 distinct signal types to register as graph
        if avg_signals < 2:
            return 0.0
        return min(avg_signals / 4.0, 1.0) * 0.9  # cap at 0.9

    def _vector_score(self, dict_records: list[dict], records: list[Any]) -> float:
        """Score for vector/embedding data."""
        if not dict_records:
            return 0.0

        vector_field_hits = 0
        for r in dict_records:
            for k, v in r.items():
                if is_likely_embedding(v):
                    vector_field_hits += 1
                    break  # One embedding field per record is sufficient

        return vector_field_hits / len(dict_records) if dict_records else 0.0


def _relational_score(
    tabular: float,
    jaccard: float,
    flat: float,
    depth: int,
    max_depth: int,
) -> float:
    """Compute a relational confidence score (0–1)."""
    if jaccard < 0.5:
        return 0.0  # Too inconsistent to be relational

    # Depth penalty: nesting above threshold reduces relational score significantly.
    # Each excess level cuts score by 40% — data deeper than max_depth+1 is almost
    # certainly a document, not a table.
    depth_factor = 1.0 if depth <= max_depth else max(0.0, 1.0 - (depth - max_depth) * 0.4)

    base = tabular * 0.6 + jaccard * 0.3 + flat * 0.1
    return base * depth_factor


def _document_score(schema_variance: float, depth: int, doc_jaccard_threshold: float) -> float:
    """Compute a document confidence score (0–1).

    Two signals drive document classification:
    - Schema variance: inconsistent keys across records (Jaccard < threshold)
    - Nesting depth: deeply nested structures are almost always documents
    Both are given roughly equal weight so that a *single deeply-nested record*
    is still correctly classified even if schema variance is zero.
    """
    # Depth contribution: 0.30 per level, capped at 0.9
    depth_bonus = min(depth * 0.30, 0.9)
    # Variance contribution: high variance → document
    variance_score = min(schema_variance * 1.2, 1.0)

    score = variance_score * 0.40 + depth_bonus * 0.60
    return min(score, 1.0)
