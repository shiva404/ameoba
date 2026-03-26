"""Integration tests for SchemaRegistry backed by real DuckDB."""

from __future__ import annotations

import pytest
import pytest_asyncio

from ameoba.domain.schema import SchemaCompatibility
from ameoba.schema.registry import SchemaRegistry


@pytest_asyncio.fixture
async def registry(kernel):
    """Use the kernel's own schema registry (backed by the test DuckDB)."""
    return kernel.schema_registry


# ---------------------------------------------------------------------------
# Basic registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_register_creates_version_1(registry):
    records = [{"id": "1", "name": "Alice"}, {"id": "2", "name": "Bob"}]
    version = await registry.register_from_records("users", records)
    assert version.collection == "users"
    assert version.version_number == 1
    assert version.field_count == 2


@pytest.mark.asyncio
async def test_register_identical_schema_returns_same_version(registry):
    records = [{"id": "1", "value": 42}]
    v1 = await registry.register_from_records("metrics", records)
    v2 = await registry.register_from_records("metrics", records)
    # Same schema → same version, no new row
    assert v1.version_number == v2.version_number
    assert v1.id == v2.id


@pytest.mark.asyncio
async def test_register_additive_schema_creates_new_version(registry):
    v1 = await registry.register_from_records(
        "products", [{"id": "1", "name": "Widget"}]
    )
    assert v1.version_number == 1

    # Add a new optional field
    v2 = await registry.register_from_records(
        "products", [{"id": "1", "name": "Widget", "price": 9.99}]
    )
    assert v2.version_number == 2
    assert v2.compatibility == SchemaCompatibility.BACKWARD_COMPATIBLE


@pytest.mark.asyncio
async def test_register_breaking_schema_flagged(registry):
    await registry.register_from_records(
        "orders", [{"order_id": "001", "amount": 100}]
    )
    # Remove the 'amount' field — breaking change
    v2 = await registry.register_from_records(
        "orders", [{"order_id": "002"}]
    )
    assert v2.compatibility == SchemaCompatibility.BREAKING


# ---------------------------------------------------------------------------
# Retrieval
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_get_latest_returns_newest_version(registry):
    await registry.register_from_records("events", [{"type": "click"}])
    await registry.register_from_records("events", [{"type": "click", "user": "Alice"}])
    latest = await registry.get_latest("events")
    assert latest is not None
    assert latest.version_number == 2


@pytest.mark.asyncio
async def test_get_latest_nonexistent_collection_returns_none(registry):
    result = await registry.get_latest("no_such_collection")
    assert result is None


@pytest.mark.asyncio
async def test_list_versions_newest_first(registry):
    await registry.register_from_records("logs", [{"level": "info"}])
    await registry.register_from_records("logs", [{"level": "info", "msg": "hello"}])
    versions = await registry.list_versions("logs")
    assert len(versions) == 2
    assert versions[0].version_number == 2  # newest first
    assert versions[1].version_number == 1


@pytest.mark.asyncio
async def test_list_collections_returns_registered(registry):
    await registry.register_from_records("col_a", [{"x": 1}])
    await registry.register_from_records("col_b", [{"y": 2}])
    collections = await registry.list_collections()
    assert "col_a" in collections
    assert "col_b" in collections


# ---------------------------------------------------------------------------
# Schema metrics
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_schema_metrics_recorded(registry):
    records = [{"id": str(i), "value": i} for i in range(10)]
    version = await registry.register_from_records("metrics_test", records)
    assert version.field_count == 2
    assert version.record_count_at_inference == 10
    assert 0.0 <= version.key_consistency_score <= 1.0


# ---------------------------------------------------------------------------
# Kernel integration — ingest auto-registers schema
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kernel_ingest_auto_registers_schema(kernel):
    from ameoba.domain.record import DataRecord

    record = DataRecord(
        collection="auto_schema_test",
        payload={"user_id": "u1", "event": "login"},
    )
    await kernel.ingest(record)

    version = await kernel.schema_registry.get_latest("auto_schema_test")
    assert version is not None
    assert version.collection == "auto_schema_test"
    assert version.field_count >= 2
