"""gRPC AuditServicer — streams audit events and verifies integrity."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import structlog

from ameoba.kernel.kernel import AmeobaKernel

logger = structlog.get_logger(__name__)


class AuditServicer:
    """gRPC servicer for the AuditService."""

    def __init__(self, kernel: AmeobaKernel) -> None:
        self._kernel = kernel

    async def Tail(
        self,
        request: Any,
        context: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream audit events in sequence order."""
        if self._kernel.audit_ledger is None:
            return

        after_seq = int(getattr(request, "after_sequence", 0) or 0)
        limit = int(getattr(request, "limit", 100) or 100)
        tenant_id = getattr(request, "tenant_id", None) or None

        async for event in self._kernel.audit_ledger.tail(
            after_sequence=after_seq,
            limit=limit,
            tenant_id=tenant_id,
        ):
            yield {
                "id": str(event.id),
                "sequence": event.sequence,
                "kind": event.kind.value,
                "occurred_at": event.occurred_at.isoformat(),
                "agent_id": event.agent_id or "",
                "tenant_id": event.tenant_id,
                "record_id": str(event.record_id) if event.record_id else "",
                "collection": event.collection or "",
                "backend_id": event.backend_id or "",
                "event_hash": event.event_hash or "",
                "previous_hash": event.previous_hash or "",
                "detail_json": json.dumps(event.detail, default=str),
            }

    async def Verify(self, request: Any, context: Any) -> dict[str, Any]:
        """Verify audit integrity and return the Merkle root."""
        ok, detail = await self._kernel.audit_verify()
        root_hash = (
            self._kernel.audit_ledger.root_hash
            if self._kernel.audit_ledger else ""
        )
        seq = (
            self._kernel.audit_ledger.sequence
            if self._kernel.audit_ledger else 0
        )
        return {
            "ok": ok,
            "detail": detail,
            "root_hash": root_hash,
            "sequence": seq,
        }
