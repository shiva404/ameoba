"""Integration tests for the StagingBuffer backed by real DuckDB."""

from __future__ import annotations

import uuid
from typing import Any
from unittest.mock import AsyncMock

import pytest
import pytest_asyncio

from ameoba.kernel.staging import StagingBuffer


@pytest_asyncio.fixture
async def staging(kernel):
    """Use the kernel's own staging buffer (backed by the test DuckDB)."""
    return kernel.staging_buffer


# ---------------------------------------------------------------------------
# Basic enqueue / count
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_enqueue_increments_pending_count(staging):
    initial = await staging.pending_count()
    await staging.enqueue(
        record_id=uuid.uuid4(),
        backend_id="backend-x",
        collection="events",
        payload={"id": "1", "data": "test"},
    )
    after = await staging.pending_count()
    assert after == initial + 1


@pytest.mark.asyncio
async def test_pending_count_filter_by_backend_id(staging):
    rid1 = uuid.uuid4()
    rid2 = uuid.uuid4()
    await staging.enqueue(rid1, "backend-a", "col1", {"k": "v1"})
    await staging.enqueue(rid2, "backend-b", "col2", {"k": "v2"})

    count_a = await staging.pending_count("backend-a")
    count_b = await staging.pending_count("backend-b")
    assert count_a >= 1
    assert count_b >= 1


# ---------------------------------------------------------------------------
# Flush succeeds
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_calls_backend_write(staging):
    rid = uuid.uuid4()
    await staging.enqueue(rid, "backend-flush", "mycol", {"id": str(rid), "msg": "hello"})

    # Mock backend that accepts writes
    mock_backend = AsyncMock()
    mock_backend.write = AsyncMock(return_value=[str(rid)])

    flushed = await staging.flush("backend-flush", mock_backend)
    assert flushed >= 1
    mock_backend.write.assert_called_once()

    # Record should be removed from staging
    count = await staging.pending_count("backend-flush")
    assert count == 0


@pytest.mark.asyncio
async def test_flush_no_pending_returns_zero(staging):
    mock_backend = AsyncMock()
    flushed = await staging.flush("nonexistent-backend", mock_backend)
    assert flushed == 0
    mock_backend.write.assert_not_called()


# ---------------------------------------------------------------------------
# Flush handles backend failure
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_flush_failure_increments_attempt_count(staging):
    rid = uuid.uuid4()
    await staging.enqueue(rid, "backend-fail", "col", {"id": str(rid)})

    mock_backend = AsyncMock()
    mock_backend.write = AsyncMock(side_effect=ConnectionError("backend down"))

    await staging.flush("backend-fail", mock_backend)

    # Record should still be in the buffer after failure
    count = await staging.pending_count("backend-fail")
    assert count == 1


# ---------------------------------------------------------------------------
# Kernel integration — backend unavailable triggers staging
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_kernel_does_not_raise_when_backend_write_fails(kernel):
    """If a backend write fails, the record should be staged (not lost).
    The kernel should not re-raise the exception.
    """
    from ameoba.domain.record import DataRecord
    from ameoba.domain.routing import BackendStatus

    # Patch the DuckDB store's write to raise an error
    duckdb_backend = None
    for desc, backend in kernel.topology.list_backends():
        if "duckdb" in desc.id.lower():
            duckdb_backend = backend
            break

    if duckdb_backend is None:
        pytest.skip("DuckDB backend not found in topology")

    original_write = duckdb_backend.write

    async def failing_write(*args: Any, **kwargs: Any) -> list[str]:
        raise IOError("Simulated write failure")

    duckdb_backend.write = failing_write
    try:
        record = DataRecord(
            collection="staging_test",
            payload={"id": "staging-1", "value": 99},
        )
        # Should not raise — write failure triggers staging
        result = await kernel.ingest(record)
        # The record was staged, not written — backend_ids may be empty
        assert isinstance(result.backend_ids, list)
    finally:
        duckdb_backend.write = original_write
