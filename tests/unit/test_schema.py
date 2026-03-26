"""Unit tests for schema inference, merging, compatibility, and metrics."""

from __future__ import annotations

import pytest

from ameoba.domain.schema import SchemaCompatibility
from ameoba.schema.compatibility import check_compatibility
from ameoba.schema.inference import (
    compute_schema_metrics,
    extract_schema,
    infer_schema,
    merge_schemas,
)


# ---------------------------------------------------------------------------
# extract_schema
# ---------------------------------------------------------------------------


def test_extract_schema_flat_dict():
    record = {"id": "1", "score": 3.14, "active": True}
    schema = extract_schema(record)
    assert schema["type"] == "object"
    assert schema["properties"]["id"] == {"type": "string"}
    assert schema["properties"]["score"] == {"type": "number"}
    assert schema["properties"]["active"] == {"type": "boolean"}
    assert set(schema["required"]) == {"id", "score", "active"}


def test_extract_schema_nested():
    record = {"user": {"name": "Alice", "age": 30}}
    schema = extract_schema(record)
    user_schema = schema["properties"]["user"]
    assert user_schema["type"] == "object"
    assert user_schema["properties"]["name"] == {"type": "string"}
    assert user_schema["properties"]["age"] == {"type": "integer"}


def test_extract_schema_array_with_items():
    record = {"tags": ["python", "ml"]}
    schema = extract_schema(record)
    tags = schema["properties"]["tags"]
    assert tags["type"] == "array"
    assert tags["items"]["type"] == "string"


def test_extract_schema_empty_array():
    schema = extract_schema({"items": []})
    assert schema["properties"]["items"] == {"type": "array"}


def test_extract_schema_null_value():
    schema = extract_schema({"field": None})
    assert schema["properties"]["field"] == {"type": "null"}


# ---------------------------------------------------------------------------
# merge_schemas (type widening)
# ---------------------------------------------------------------------------


def test_merge_schemas_same_type():
    a = {"type": "string"}
    b = {"type": "string"}
    assert merge_schemas(a, b) == {"type": "string"}


def test_merge_schemas_widening_int_to_number():
    a = {"type": "integer"}
    b = {"type": "number"}
    result = merge_schemas(a, b)
    assert result["type"] == "number"


def test_merge_schemas_objects_union_properties():
    a = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "name": {"type": "string"}},
        "required": ["id", "name"],
    }
    b = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "email": {"type": "string"}},
        "required": ["id", "email"],
    }
    result = merge_schemas(a, b)
    assert result["type"] == "object"
    # id is in both — required
    assert "id" in result["required"]
    # name and email are in only one — optional (not required)
    assert "name" not in result["required"]
    assert "email" not in result["required"]
    # But both properties are present
    assert "name" in result["properties"]
    assert "email" in result["properties"]


def test_merge_schemas_null_widens_to_string():
    a = {"type": "null"}
    b = {"type": "string"}
    result = merge_schemas(a, b)
    assert result["type"] == "string"


# ---------------------------------------------------------------------------
# infer_schema (batch)
# ---------------------------------------------------------------------------


def test_infer_schema_single_record():
    records = [{"id": "1", "value": 42}]
    schema = infer_schema(records)
    assert schema["type"] == "object"
    assert "id" in schema["properties"]
    assert "value" in schema["properties"]


def test_infer_schema_multiple_records_type_widening():
    records = [
        {"score": 5},      # integer
        {"score": 3.14},   # number
    ]
    schema = infer_schema(records)
    # score should widen to number
    assert schema["properties"]["score"]["type"] == "number"


def test_infer_schema_optional_fields_not_required():
    records = [
        {"id": "1", "name": "Alice"},
        {"id": "2"},  # no name
    ]
    schema = infer_schema(records)
    assert "id" in schema["required"]
    assert "name" not in schema["required"]
    assert "name" in schema["properties"]


def test_infer_schema_empty_list():
    schema = infer_schema([])
    assert schema["type"] == "object"


# ---------------------------------------------------------------------------
# check_compatibility
# ---------------------------------------------------------------------------


def test_compatibility_identical():
    schema = {"type": "object", "properties": {"id": {"type": "string"}}, "required": ["id"]}
    assert check_compatibility(schema, schema) == SchemaCompatibility.IDENTICAL


def test_compatibility_backward_compatible_add_field():
    old = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    new = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "email": {"type": "string"}},
        "required": ["id"],
    }
    result = check_compatibility(old, new)
    assert result == SchemaCompatibility.BACKWARD_COMPATIBLE


def test_compatibility_breaking_removed_field():
    old = {
        "type": "object",
        "properties": {"id": {"type": "string"}, "name": {"type": "string"}},
        "required": ["id", "name"],
    }
    new = {
        "type": "object",
        "properties": {"id": {"type": "string"}},
        "required": ["id"],
    }
    result = check_compatibility(old, new)
    assert result == SchemaCompatibility.BREAKING


def test_compatibility_breaking_type_narrowed():
    old = {"type": "object", "properties": {"score": {"type": "number"}}, "required": ["score"]}
    new = {"type": "object", "properties": {"score": {"type": "integer"}}, "required": ["score"]}
    result = check_compatibility(old, new)
    assert result == SchemaCompatibility.BREAKING


# ---------------------------------------------------------------------------
# compute_schema_metrics
# ---------------------------------------------------------------------------


def test_schema_metrics_basic():
    records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    schema = infer_schema(records)
    metrics = compute_schema_metrics(schema, records)
    assert metrics["field_count"] == 2.0
    assert metrics["key_consistency_score"] == 1.0  # all records have both fields
    assert metrics["nesting_depth"] >= 1


def test_schema_metrics_optional_field_reduces_consistency():
    records = [{"id": "1", "name": "Alice"}, {"id": "2"}]
    schema = infer_schema(records)
    # Required = {"id"} (intersection)
    # All records have "id", so consistency = 1.0
    metrics = compute_schema_metrics(schema, records)
    assert metrics["key_consistency_score"] == 1.0  # only "id" is required


def test_schema_metrics_empty_records():
    schema = infer_schema([])
    metrics = compute_schema_metrics(schema, [])
    assert metrics["field_count"] == 0.0
    assert metrics["key_consistency_score"] == 1.0
