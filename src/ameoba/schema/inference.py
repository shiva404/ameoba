"""Schema inference — Spark-style map-reduce over a stream of records.

Each record contributes a per-record schema.  Schemas are merged using a
lattice-based type unification rule (widening types rather than conflicting).

The inferred schema is a JSON Schema dict suitable for storage in the registry.

References:
    Baazizi et al. EDBT 2017 — JSON schema inference for semi-structured data
    Spark schema inference — mapPartitions + reduce with StructType merging
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any


# ---------------------------------------------------------------------------
# JSON Schema type lattice (widening order — never narrowing)
# ---------------------------------------------------------------------------

# Maps Python type → JSON Schema type string
_PY_TO_JSON_TYPE: dict[type, str] = {
    bool: "boolean",  # must come before int (bool is a subclass of int)
    int: "integer",
    float: "number",
    str: "string",
    bytes: "string",   # encoded as base64 string in JSON
    list: "array",
    dict: "object",
    type(None): "null",
}

# Widening order: if we see both types in different records, use the wider one
_TYPE_WIDENING: dict[tuple[str, str], str] = {
    ("integer", "number"): "number",
    ("number", "integer"): "number",
    ("integer", "string"): "string",
    ("number", "string"): "string",
    ("boolean", "integer"): "integer",
    ("integer", "boolean"): "integer",
    ("boolean", "string"): "string",
    ("null", "string"): "string",
    ("string", "null"): "string",
    ("null", "integer"): "integer",
    ("integer", "null"): "integer",
    ("null", "number"): "number",
    ("number", "null"): "number",
    ("null", "boolean"): "boolean",
    ("boolean", "null"): "boolean",
    ("null", "array"): "array",
    ("array", "null"): "array",
    ("null", "object"): "object",
    ("object", "null"): "object",
}


def _py_type_to_json(v: Any) -> str:
    for py_type, json_type in _PY_TO_JSON_TYPE.items():
        if isinstance(v, py_type):
            return json_type
    return "string"  # fallback


def _widen(a: str, b: str) -> str:
    """Return the wider of two JSON Schema type strings."""
    if a == b:
        return a
    return _TYPE_WIDENING.get((a, b), _TYPE_WIDENING.get((b, a), "string"))


# ---------------------------------------------------------------------------
# Per-record schema extraction
# ---------------------------------------------------------------------------

def extract_schema(record: dict[str, Any], *, depth: int = 0, max_depth: int = 5) -> dict[str, Any]:
    """Extract a JSON Schema fragment from a single record.

    Recursively descends into nested objects up to max_depth.
    """
    if not isinstance(record, dict):
        return {"type": _py_type_to_json(record)}

    properties: dict[str, Any] = {}
    for key, value in record.items():
        if depth < max_depth and isinstance(value, dict):
            properties[key] = extract_schema(value, depth=depth + 1, max_depth=max_depth)
        elif depth < max_depth and isinstance(value, list) and value:
            first = value[0]
            item_schema = extract_schema(first, depth=depth + 1, max_depth=max_depth)
            properties[key] = {"type": "array", "items": item_schema}
        elif isinstance(value, list):
            properties[key] = {"type": "array"}
        else:
            properties[key] = {"type": _py_type_to_json(value)}

    return {
        "type": "object",
        "properties": properties,
        "required": list(record.keys()),
    }


# ---------------------------------------------------------------------------
# Schema merging (lattice unification)
# ---------------------------------------------------------------------------

def merge_schemas(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    """Merge two JSON Schema dicts using the widening lattice.

    Rules:
    - Same type → keep.
    - Different primitive types → widen.
    - One is object, other isn't → use object (most flexible).
    - Both are objects → recursively merge properties.
    - Required fields: intersection (a field is required only if required in both).
    """
    if a.get("type") == "object" and b.get("type") == "object":
        return _merge_objects(a, b)

    type_a = a.get("type", "string")
    type_b = b.get("type", "string")

    if type_a == type_b:
        if type_a == "array":
            # Merge array item schemas
            items_a = a.get("items", {})
            items_b = b.get("items", {})
            if items_a and items_b:
                return {"type": "array", "items": merge_schemas(items_a, items_b)}
        return a

    return {"type": _widen(type_a, type_b)}


def _merge_objects(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    props_a = a.get("properties", {})
    props_b = b.get("properties", {})
    req_a = set(a.get("required", []))
    req_b = set(b.get("required", []))

    all_keys = set(props_a) | set(props_b)
    merged_props: dict[str, Any] = {}
    for key in all_keys:
        if key in props_a and key in props_b:
            merged_props[key] = merge_schemas(props_a[key], props_b[key])
        elif key in props_a:
            # Key only in a — optional (not in all records)
            merged_props[key] = props_a[key]
        else:
            merged_props[key] = props_b[key]

    required = list(req_a & req_b)  # Intersection — must appear in all records
    return {"type": "object", "properties": merged_props, "required": required}


# ---------------------------------------------------------------------------
# Batch inference (map-reduce)
# ---------------------------------------------------------------------------

def infer_schema(records: list[dict[str, Any]]) -> dict[str, Any]:
    """Infer a unified JSON Schema from a list of records.

    Uses a sequential reduce (suitable for small batches).
    For streaming/large-scale use, call reduce(map(extract_schema, chunk), merge_schemas).
    """
    if not records:
        return {"type": "object", "properties": {}}

    dict_records = [r for r in records if isinstance(r, dict)]
    if not dict_records:
        return {"type": _py_type_to_json(records[0])}

    # Map phase
    per_record = [extract_schema(r) for r in dict_records]

    # Reduce phase
    result = per_record[0]
    for schema in per_record[1:]:
        result = merge_schemas(result, schema)

    return result


# ---------------------------------------------------------------------------
# Schema metrics (feed back into classification)
# ---------------------------------------------------------------------------

def compute_schema_metrics(schema: dict[str, Any], records: list[dict[str, Any]]) -> dict[str, float]:
    """Compute metrics on an inferred schema for the schema registry."""
    props = schema.get("properties", {})
    field_count = len(props)

    # Key consistency: fraction of records that have all required fields
    required = set(schema.get("required", []))
    if required and records:
        hits = sum(
            1 for r in records
            if isinstance(r, dict) and required.issubset(r.keys())
        )
        key_consistency = hits / len(records)
    else:
        key_consistency = 1.0

    # Nesting depth: max depth in properties
    max_depth = _max_schema_depth(schema)

    # Complexity: normalised (field_count * depth / 100)
    complexity = min(field_count * max_depth / 100.0, 1.0)

    return {
        "field_count": float(field_count),
        "nesting_depth": float(max_depth),
        "key_consistency_score": key_consistency,
        "complexity_score": complexity,
    }


def _max_schema_depth(schema: dict[str, Any], depth: int = 0) -> int:
    if schema.get("type") != "object":
        return depth
    props = schema.get("properties", {})
    if not props:
        return depth + 1
    return max(_max_schema_depth(v, depth + 1) for v in props.values())
