"""JSONL (newline-delimited JSON) audit exporter.

Universal format — works with any SIEM, log aggregator, or custom processor.
Includes all event fields in a flat, easily parseable structure.
"""

from __future__ import annotations

import json
from typing import AsyncIterator

from ameoba.domain.audit import AuditEvent


def event_to_jsonl(event: AuditEvent) -> str:
    """Serialise a single AuditEvent to a JSONL line."""
    return json.dumps({
        "id": str(event.id),
        "sequence": event.sequence,
        "kind": event.kind.value,
        "occurred_at": event.occurred_at.isoformat(),
        "agent_id": event.agent_id,
        "session_id": event.session_id,
        "tenant_id": event.tenant_id,
        "record_id": str(event.record_id) if event.record_id else None,
        "collection": event.collection,
        "backend_id": event.backend_id,
        "detail": event.detail,
        "event_hash": event.event_hash,
        "previous_hash": event.previous_hash,
    }, default=str)


async def export_jsonl(
    sink: object,
    *,
    after_sequence: int = 0,
    limit: int = 10_000,
    tenant_id: str | None = None,
) -> AsyncIterator[str]:
    """Stream audit events as JSONL lines from the given sink."""
    async for event in sink.tail(  # type: ignore[attr-defined]
        after_sequence=after_sequence,
        limit=limit,
        tenant_id=tenant_id,
    ):
        yield event_to_jsonl(event) + "\n"
