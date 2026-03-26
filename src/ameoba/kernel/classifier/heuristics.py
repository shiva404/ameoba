"""Scoring formulas shared across classification layers.

All functions are pure (no I/O, no side effects) and designed to be fast.
"""

from __future__ import annotations

import math
import struct
from collections import Counter
from typing import Any, Sequence


# ---------------------------------------------------------------------------
# Entropy analysis
# ---------------------------------------------------------------------------

def shannon_entropy_bytes(data: bytes, sample_size: int = 65536) -> float:
    """Compute Shannon entropy in bits-per-byte for a byte sequence.

    A value close to 8.0 indicates near-random / compressed / encrypted data.
    Typical text hovers around 4–5.  Structured binary formats land at 5–7.

    Args:
        data:        Raw bytes to analyse.
        sample_size: Cap the analysis at this many bytes for speed.

    Returns:
        Entropy in bits-per-byte (0.0 – 8.0).
    """
    sample = data[:sample_size]
    if not sample:
        return 0.0

    counts = Counter(sample)
    n = len(sample)
    entropy = -sum((c / n) * math.log2(c / n) for c in counts.values() if c > 0)
    return entropy


def null_byte_fraction(data: bytes, sample_size: int = 65536) -> float:
    """Fraction of null (0x00) bytes in a sample.

    Binary files often have significant null-byte presence (padding, etc.).
    """
    sample = data[:sample_size]
    if not sample:
        return 0.0
    return sample.count(0x00) / len(sample)


# ---------------------------------------------------------------------------
# Structural heuristics for dicts / lists
# ---------------------------------------------------------------------------

def jaccard_key_similarity(records: Sequence[dict[str, Any]]) -> float:
    """Measure how consistently records share the same keys.

    Jaccard similarity = |intersection| / |union| across all record key sets.
    Returns 1.0 for perfectly consistent schemas, 0.0 for no overlap.

    Args:
        records: A sample of dict records.

    Returns:
        Jaccard similarity in [0.0, 1.0].
    """
    if not records:
        return 0.0
    if len(records) == 1:
        return 1.0

    key_sets = [frozenset(r.keys()) for r in records if isinstance(r, dict)]
    if not key_sets:
        return 0.0

    intersection = key_sets[0]
    union = key_sets[0]
    for ks in key_sets[1:]:
        intersection = intersection & ks
        union = union | ks

    return len(intersection) / len(union) if union else 0.0


def max_nesting_depth(obj: Any, _depth: int = 0) -> int:
    """Compute the maximum nesting depth of a JSON-like structure."""
    if isinstance(obj, dict):
        if not obj:
            return _depth
        return max(max_nesting_depth(v, _depth + 1) for v in obj.values())
    if isinstance(obj, list):
        if not obj:
            return _depth
        return max(max_nesting_depth(item, _depth + 1) for item in obj)
    return _depth


def type_homogeneity(values: Sequence[Any]) -> float:
    """Fraction of values that share the most-common Python type.

    A column where all values are ``int`` scores 1.0.  A mixed column scores < 1.0.
    """
    if not values:
        return 1.0
    type_counts = Counter(type(v).__name__ for v in values)
    most_common_count = type_counts.most_common(1)[0][1]
    return most_common_count / len(values)


def flatness_ratio(records: Sequence[dict[str, Any]]) -> float:
    """Ratio of records whose max nesting depth is 1 (all values are scalars).

    A fully flat dataset (like CSV) scores 1.0.
    """
    if not records:
        return 0.0
    flat = sum(1 for r in records if isinstance(r, dict) and max_nesting_depth(r) <= 1)
    return flat / len(records)


# ---------------------------------------------------------------------------
# Composite scores
# ---------------------------------------------------------------------------

def tabular_score(
    records: Sequence[dict[str, Any]],
    *,
    jaccard_weight: float = 0.5,
    flatness_weight: float = 0.3,
    homogeneity_weight: float = 0.2,
) -> float:
    """Composite score for how 'relational/tabular' a record set is.

    Returns a value in [0.0, 1.0].
    """
    if not records:
        return 0.0

    jaccard = jaccard_key_similarity(records)
    flat = flatness_ratio(records)

    # Per-column type homogeneity averaged across all columns
    if records and isinstance(records[0], dict):
        all_keys: set[str] = set()
        for r in records:
            if isinstance(r, dict):
                all_keys.update(r.keys())
        col_scores = []
        for key in all_keys:
            vals = [r[key] for r in records if isinstance(r, dict) and key in r]
            col_scores.append(type_homogeneity(vals))
        avg_homogeneity = sum(col_scores) / len(col_scores) if col_scores else 0.0
    else:
        avg_homogeneity = 0.0

    return (
        jaccard * jaccard_weight
        + flat * flatness_weight
        + avg_homogeneity * homogeneity_weight
    )


# ---------------------------------------------------------------------------
# Graph-pattern detection helpers
# ---------------------------------------------------------------------------

# Vocabulary of field names that strongly suggest graph structure
_GRAPH_NODE_VOCAB = frozenset({"nodes", "vertices", "node", "vertex"})
_GRAPH_EDGE_VOCAB = frozenset({"edges", "links", "relationships", "edge", "link", "relationship"})
_GRAPH_TRIPLE_VOCAB = frozenset({"subject", "predicate", "object"})
_GRAPH_ENDPOINT_VOCAB = frozenset({
    "source", "target", "from", "to", "src", "dst",
    "source_id", "target_id", "from_id", "to_id",
})


def graph_signal_count(obj: Any) -> int:
    """Count the number of distinct graph vocabulary signals in the structure.

    Requires multiple signals to avoid false positives from relational FKs.

    Returns:
        Number of distinct signal types found (0–4).
    """
    if not isinstance(obj, (dict, list)):
        return 0

    keys: set[str] = set()
    if isinstance(obj, dict):
        keys = {k.lower() for k in obj}
        # Recurse one level for wrapped formats like {"graph": {...}}
        for v in obj.values():
            if isinstance(v, dict):
                keys |= {k.lower() for k in v}

    signals = 0
    if keys & _GRAPH_NODE_VOCAB:
        signals += 1
    if keys & _GRAPH_EDGE_VOCAB:
        signals += 1
    if keys & _GRAPH_TRIPLE_VOCAB:
        signals += 1
    if keys & _GRAPH_ENDPOINT_VOCAB:
        signals += 1

    return signals


# ---------------------------------------------------------------------------
# Vector / embedding detection
# ---------------------------------------------------------------------------

# Common embedding dimensions from well-known models
_KNOWN_EMBEDDING_DIMS = frozenset({
    64, 128, 256, 384, 512, 768, 1024, 1536, 2048, 3072, 4096,
})

_VECTOR_FIELD_NAMES = frozenset({
    "embedding", "embeddings", "vector", "vectors",
    "emb", "embs", "representation", "representations",
    "dense_vector", "sparse_vector",
})


def is_likely_embedding(value: Any) -> bool:
    """True if the value looks like a numerical embedding vector."""
    if not isinstance(value, (list, tuple)):
        return False
    if len(value) not in _KNOWN_EMBEDDING_DIMS:
        return False
    if not value:
        return False
    # First few elements should be floats
    sample = value[:8]
    return all(isinstance(x, (int, float)) for x in sample)
