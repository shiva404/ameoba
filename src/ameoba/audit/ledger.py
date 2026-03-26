"""AuditLedger — the in-process coordinator between the event stream and the sink.

The ledger:
1. Assigns gapless sequence numbers.
2. Computes the SHA-256 hash chain (each event hashes in the previous hash).
3. Feeds canonical event bytes to the in-memory Merkle tree.
4. Delegates persistence to the injected AuditSink implementation.

This class is NOT the sink itself — it is a thin coordinator that adds
ordering and hashing invariants before calling the sink.
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from datetime import datetime, timezone
from typing import AsyncIterator

import structlog

from ameoba.audit.merkle import MerkleTree, leaf_hash
from ameoba.domain.audit import AuditEvent, AuditEventKind, MerkleNode

logger = structlog.get_logger(__name__)


def _canonical_bytes(event: AuditEvent) -> bytes:
    """Deterministic serialisation used for hashing.

    We serialise only the immutable content fields (not sequence/hashes)
    so that the hash is stable regardless of when it is computed.
    """
    doc = {
        "id": str(event.id),
        "kind": event.kind.value,
        "occurred_at": event.occurred_at.isoformat(),
        "agent_id": event.agent_id,
        "tenant_id": event.tenant_id,
        "record_id": str(event.record_id) if event.record_id else None,
        "collection": event.collection,
        "backend_id": event.backend_id,
        "detail": event.detail,
    }
    return json.dumps(doc, sort_keys=True, default=str).encode("utf-8")


class AuditLedger:
    """Thread-safe, in-process audit ledger coordinator.

    Example::

        ledger = AuditLedger(sink=sqlite_sink)
        event = await ledger.record(
            kind=AuditEventKind.INGESTION,
            agent_id="agent-1",
            record_id=some_uuid,
        )
    """

    def __init__(self, sink: object) -> None:  # sink: AuditSink
        self._sink = sink
        self._sequence = 0
        self._previous_hash: str = hashlib.sha256(b"genesis").hexdigest()
        self._merkle = MerkleTree()
        self._lock = asyncio.Lock()

    async def record(
        self,
        kind: AuditEventKind,
        *,
        agent_id: str | None = None,
        session_id: str | None = None,
        tenant_id: str = "default",
        record_id: object = None,  # uuid.UUID | None
        collection: str | None = None,
        backend_id: str | None = None,
        detail: dict | None = None,
    ) -> AuditEvent:
        """Create, hash-chain, and persist a new audit event.

        Returns:
            The fully enriched event (with sequence, hashes).

        This is the primary write path — callers should not construct
        AuditEvents themselves.
        """
        import uuid as _uuid

        event = AuditEvent(
            id=_uuid.uuid4(),
            kind=kind,
            occurred_at=datetime.now(timezone.utc),
            agent_id=agent_id,
            session_id=session_id,
            tenant_id=tenant_id,
            record_id=record_id,  # type: ignore[arg-type]
            collection=collection,
            backend_id=backend_id,
            detail=detail or {},
        )

        async with self._lock:
            self._sequence += 1
            seq = self._sequence

            canonical = _canonical_bytes(event)
            event_hash = leaf_hash(canonical)
            self._merkle.append(canonical)

            enriched = event.model_copy(update={
                "sequence": seq,
                "previous_hash": self._previous_hash,
                "event_hash": event_hash,
            })
            self._previous_hash = event_hash

        # Persist (outside the lock — the sink is responsible for its own safety)
        await self._sink.append(enriched)  # type: ignore[attr-defined]

        log = logger.bind(
            event_id=str(enriched.id),
            seq=seq,
            kind=kind.value,
            tenant_id=tenant_id,
        )
        log.debug("audit_event_recorded")
        return enriched

    @property
    def sequence(self) -> int:
        """Current highest sequence number (0 = nothing written yet)."""
        return self._sequence

    @property
    def root_hash(self) -> str:
        """Current Merkle root hash."""
        return self._merkle.root

    async def verify_integrity(self) -> tuple[bool, str]:
        """Re-validate chain hashes and Merkle tree from the persisted sink.

        Returns:
            (ok, detail_message)
        """
        return await self._sink.verify_integrity()  # type: ignore[attr-defined]

    async def tail(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        tenant_id: str | None = None,
    ) -> AsyncIterator[AuditEvent]:
        """Stream events from the sink in sequence order."""
        async for event in self._sink.tail(  # type: ignore[attr-defined]
            after_sequence=after_sequence, limit=limit, tenant_id=tenant_id
        ):
            yield event

    def get_inclusion_proof(self, index: int) -> list[str]:
        """Merkle inclusion proof for a leaf (by 0-based index = sequence - 1)."""
        return self._merkle.inclusion_proof(index)
