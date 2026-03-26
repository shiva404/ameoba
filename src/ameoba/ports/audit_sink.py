"""AuditSink protocol — the contract for any audit ledger backend."""

from __future__ import annotations

from typing import AsyncIterator, Protocol, runtime_checkable

from ameoba.domain.audit import AuditEvent, MerkleNode


@runtime_checkable
class AuditSink(Protocol):
    """Append-only audit event sink.

    The kernel emits AuditEvents and passes them here.  Implementations
    must guarantee append-only semantics — once written, events cannot
    be modified or deleted.
    """

    async def append(self, event: AuditEvent) -> AuditEvent:
        """Append an event, assigning sequence number and computing hashes.

        Returns:
            The same event enriched with ``sequence``, ``previous_hash``,
            and ``event_hash``.
        """
        ...

    async def get_root_hash(self) -> str:
        """Return the current Merkle root hash (hex).

        Used by the background verifier and for external anchoring digests.
        """
        ...

    async def verify_integrity(self) -> tuple[bool, str]:
        """Re-validate the entire chain.

        Returns:
            (ok, detail) — ok=True means no tampering detected.
        """
        ...

    async def tail(
        self,
        *,
        after_sequence: int = 0,
        limit: int = 100,
        tenant_id: str | None = None,
    ) -> AsyncIterator[AuditEvent]:
        """Stream events in sequence order."""
        ...

    async def get_inclusion_proof(self, sequence: int) -> list[MerkleNode]:
        """Return the Merkle inclusion proof for the given sequence number."""
        ...
