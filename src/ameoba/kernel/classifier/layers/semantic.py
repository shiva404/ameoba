"""Layer 3: Semantic classification.

Interprets *what the data means* — graph topology patterns, domain vocabulary,
and vector dimension recognition.  This is the most expensive layer and runs
only if earlier layers did not produce a high-confidence result.

Priority: 70 (runs after structural).
"""

from __future__ import annotations

from typing import Any

from ameoba.domain.record import ClassificationVector
from ameoba.kernel.classifier.heuristics import (
    _GRAPH_EDGE_VOCAB,
    _GRAPH_ENDPOINT_VOCAB,
    _GRAPH_NODE_VOCAB,
    _GRAPH_TRIPLE_VOCAB,
    _KNOWN_EMBEDDING_DIMS,
    _VECTOR_FIELD_NAMES,
    graph_signal_count,
    is_likely_embedding,
)

# Domain-specific vocabulary patterns — used for semantic boosting
_FINANCIAL_VOCAB = frozenset({
    "amount", "currency", "debit", "credit", "account_id", "transaction_id",
    "balance", "iban", "swift", "ledger",
})
_MEDICAL_VOCAB = frozenset({
    "patient_id", "diagnosis", "icd_code", "medication", "dosage",
    "procedure", "encounter_id", "mrn",
})
_GRAPH_PROPERTY_VOCAB = frozenset({
    "label", "labels", "type", "kind",  # Neo4j/property graph vocab
    "id", "_id", "node_id",
    "properties", "attrs",
})


class SemanticClassifier:
    """Layer 3 classifier — semantic interpretation of data meaning.

    Priority: 70.
    """

    priority: int = 70
    name: str = "semantic_classifier"

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        decoded = context.get("decoded", data)
        if decoded is None:
            return None

        records = decoded if isinstance(decoded, list) else [decoded]

        # Skip if current confidence is already high
        if context.get("current_confidence", 0.0) > 0.9:
            return None

        scores: dict[str, float] = {
            "relational": 0.0,
            "document": 0.0,
            "graph": 0.0,
            "blob": 0.0,
            "vector": 0.0,
        }

        graph_boost = self._check_graph_topology(records)
        vector_boost = self._check_vector_structure(records)
        domain_boost = self._check_domain_vocabulary(records)

        if graph_boost > 0:
            scores["graph"] = graph_boost
        if vector_boost > 0:
            scores["vector"] = vector_boost
        if domain_boost:
            # Domain vocabulary makes relational more likely
            scores["relational"] = max(scores["relational"], domain_boost)

        if all(v == 0.0 for v in scores.values()):
            return None  # No semantic signal — fall through to fallback

        return ClassificationVector(
            relational=scores["relational"],
            document=scores["document"],
            graph=scores["graph"],
            blob=scores["blob"],
            vector=scores["vector"],
            confidence=max(scores.values()),
            dominant_layer=self.name,
        )

    def _check_graph_topology(self, records: list[Any]) -> float:
        """Detect property-graph or triple-store patterns."""
        if not records:
            return 0.0

        sample = records[:20]
        signal_sum = sum(graph_signal_count(r) for r in sample)
        avg_signals = signal_sum / len(sample)

        # Additional property-graph check: nodes list + edges list at top level
        if isinstance(records[0], dict):
            top_keys = {k.lower() for k in records[0]}
            has_nodes = bool(top_keys & _GRAPH_NODE_VOCAB)
            has_edges = bool(top_keys & _GRAPH_EDGE_VOCAB)
            has_endpoints = bool(top_keys & _GRAPH_ENDPOINT_VOCAB)
            has_triple = bool(top_keys & _GRAPH_TRIPLE_VOCAB)

            if has_nodes and has_edges:
                return 0.95  # Very strong graph signal
            if has_triple and len(top_keys & _GRAPH_TRIPLE_VOCAB) == 3:
                return 0.90  # Full SPO triple
            if has_endpoints and avg_signals >= 2:
                return 0.75

        return min(avg_signals / 4.0, 0.65) if avg_signals >= 2 else 0.0

    def _check_vector_structure(self, records: list[Any]) -> float:
        """Detect embedding vectors in named fields or as raw arrays."""
        if not records:
            return 0.0

        # Case 1: Records have named vector fields
        if isinstance(records[0], dict):
            hit_count = 0
            for rec in records[:10]:
                for key, val in rec.items():
                    if key.lower() in _VECTOR_FIELD_NAMES and is_likely_embedding(val):
                        hit_count += 1
                        break
            if hit_count > 0:
                return 0.9 * (hit_count / min(len(records), 10))

        # Case 2: The whole record IS an embedding (raw float array)
        sample = records[:5]
        if all(is_likely_embedding(r) for r in sample):
            return 0.85

        return 0.0

    def _check_domain_vocabulary(self, records: list[Any]) -> float:
        """Boost relational score for known domain-specific vocabularies."""
        if not isinstance(records[0], dict) if records else True:
            return 0.0

        all_keys = {k.lower() for r in records[:10] if isinstance(r, dict) for k in r}

        financial_hits = len(all_keys & _FINANCIAL_VOCAB)
        medical_hits = len(all_keys & _MEDICAL_VOCAB)

        if financial_hits >= 3 or medical_hits >= 3:
            return 0.7  # Strong domain match → relational
        if financial_hits >= 1 or medical_hits >= 1:
            return 0.4

        return 0.0
