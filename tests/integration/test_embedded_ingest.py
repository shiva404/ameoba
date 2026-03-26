"""Integration tests for the embedded ingest pipeline.

These tests run against real DuckDB and SQLite instances in a temp directory.
No Docker or external services required.
"""

from __future__ import annotations

import pytest

from ameoba.domain.record import DataCategory, DataLifecycle, DataRecord
from ameoba.kernel.kernel import AmeobaKernel


@pytest.mark.asyncio
async def test_ingest_relational_record(kernel: AmeobaKernel):
    record = DataRecord(
        collection="users",
        payload={"id": 1, "name": "alice", "email": "alice@example.com"},
    )
    result = await kernel.ingest(record)

    assert result.record_id == record.id
    assert result.classification.primary_category == DataCategory.RELATIONAL
    assert result.audit_sequence > 0
    assert "duckdb-embedded" in result.backend_ids


@pytest.mark.asyncio
async def test_ingest_binary_blob(kernel: AmeobaKernel):
    import os
    record = DataRecord(
        collection="uploads",
        payload=os.urandom(4096),
        content_type="application/octet-stream",
    )
    result = await kernel.ingest(record)

    assert result.classification.primary_category == DataCategory.BLOB
    assert "local-blob-embedded" in result.backend_ids


@pytest.mark.asyncio
async def test_ingest_document(kernel: AmeobaKernel):
    record = DataRecord(
        collection="reports",
        payload={
            "title": "Q4 Report",
            "sections": [
                {"heading": "Summary", "content": "...", "nested": {"deep": True}},
                {"heading": "Details", "data": [1, 2, 3], "extra": "varies"},
            ],
            "created_by": {"name": "Alice", "role": "analyst"},
        },
    )
    result = await kernel.ingest(record)
    assert result.classification.primary_category == DataCategory.DOCUMENT


@pytest.mark.asyncio
async def test_audit_trail_created(kernel: AmeobaKernel):
    """Each ingestion should produce multiple audit events."""
    initial_seq = kernel.audit_ledger.sequence  # type: ignore[union-attr]

    record = DataRecord(
        collection="events",
        payload={"event_type": "click", "user_id": "u123"},
    )
    result = await kernel.ingest(record)

    # Should have produced at least: classification + routing + write events
    assert result.audit_sequence >= initial_seq + 3


@pytest.mark.asyncio
async def test_audit_integrity_after_ingest(kernel: AmeobaKernel):
    """The audit chain should remain valid after ingestions."""
    for i in range(5):
        await kernel.ingest(DataRecord(
            collection="test",
            payload={"i": i, "value": f"item-{i}"},
        ))

    ok, detail = await kernel.audit_verify()
    assert ok, f"Audit integrity failed: {detail}"


@pytest.mark.asyncio
async def test_batch_ingest(kernel: AmeobaKernel):
    records = [
        DataRecord(
            collection="orders",
            payload={"order_id": i, "amount": i * 10.0, "status": "pending"},
        )
        for i in range(10)
    ]
    results = await kernel.ingest_batch(records)
    assert len(results) == 10
    assert all(r.audit_sequence > 0 for r in results)


@pytest.mark.asyncio
async def test_query_after_ingest(kernel: AmeobaKernel):
    """Records written via ingest should be queryable via SQL."""
    for i in range(3):
        await kernel.ingest(DataRecord(
            collection="products",
            payload={"sku": f"SKU-{i:03d}", "price": float(i * 5), "in_stock": True},
        ))

    result = await kernel.query("SELECT * FROM products")
    assert result.row_count >= 3
    assert "sku" in result.columns or "_record_id" in result.columns


@pytest.mark.asyncio
async def test_health_check(kernel: AmeobaKernel):
    health = await kernel.health()
    assert health["kernel"] == "ok"
    assert "duckdb-embedded" in health["backends"]
    assert health["backends"]["duckdb-embedded"] == "available"


@pytest.mark.asyncio
async def test_duckdb_schema_evolution_adds_columns(kernel: AmeobaKernel):
    """Second ingest with new keys should ALTER TABLE ADD COLUMN and insert."""
    await kernel.ingest(
        DataRecord(
            collection="evolve_widgets",
            payload={"sku": "A1", "price": 10},
        ),
    )
    await kernel.ingest(
        DataRecord(
            collection="evolve_widgets",
            payload={"sku": "A2", "price": 20, "warranty_years": 2},
        ),
    )
    result = await kernel.query(
        "SELECT sku, price, warranty_years FROM evolve_widgets ORDER BY sku"
    )
    assert result.row_count >= 2
    by_sku = {row[0]: row for row in result.rows}
    w_idx = result.columns.index("warranty_years")
    assert by_sku["A1"][w_idx] is None or by_sku["A1"][w_idx] == ""
    assert str(by_sku["A2"][w_idx]) == "2"
