"""Audit ledger must restore sequence from SQLite across process restarts."""

from __future__ import annotations

import pytest

from ameoba.config import EmbeddedConfig, Settings
from ameoba.domain.record import DataRecord
from ameoba.kernel.kernel import AmeobaKernel


@pytest.mark.asyncio
async def test_audit_sequence_continues_after_kernel_restart(tmp_path) -> None:
    data_dir = tmp_path / "persist"
    embedded = EmbeddedConfig(data_dir=data_dir)
    settings = Settings(embedded=embedded, environment="development", _env_file=None)

    k1 = AmeobaKernel(settings)
    await k1.start()
    assert k1.audit_ledger is not None
    seq_after_start1 = k1.audit_ledger.sequence
    await k1.ingest(DataRecord(collection="events", payload={"n": 1}))
    assert k1.audit_ledger.sequence > seq_after_start1
    await k1.stop()
    seq_persisted = k1.audit_ledger.sequence

    k2 = AmeobaKernel(settings)
    await k2.start()
    assert k2.audit_ledger is not None
    # Hydrate restores tip; start() then appends SYSTEM_START (one new sequence).
    assert k2.audit_ledger.sequence == seq_persisted + 1
    await k2.ingest(DataRecord(collection="events", payload={"n": 2}))
    assert k2.audit_ledger.sequence > seq_persisted + 1
    ok, detail = await k2.audit_verify()
    assert ok, detail
    await k2.stop()
