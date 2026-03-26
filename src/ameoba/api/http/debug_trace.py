"""Serialization helpers for debug / investor trace APIs (no HTML)."""

from __future__ import annotations

import json
import uuid
from typing import Any

from ameoba.domain.audit import AuditEvent
from ameoba.kernel.kernel import AmeobaKernel, IngestResult


def payload_preview(payload: Any, max_len: int = 4000) -> dict[str, Any]:
    """Human-readable payload summary for API / UI (no huge blobs)."""
    if isinstance(payload, dict):
        raw = json.dumps(payload, indent=2, default=str, sort_keys=True)
        truncated = len(raw) > max_len
        text = raw[:max_len] + ("…" if truncated else "")
        return {"kind": "object", "preview": text, "truncated": truncated, "keys": list(payload.keys())[:40]}
    if isinstance(payload, list):
        raw = json.dumps(payload, default=str)
        truncated = len(raw) > max_len
        return {
            "kind": "array",
            "length": len(payload),
            "preview": raw[:max_len] + ("…" if truncated else ""),
            "truncated": truncated,
        }
    if isinstance(payload, (bytes, bytearray)):
        return {"kind": "binary", "byte_length": len(payload), "preview": f"<{len(payload)} bytes>"}
    if isinstance(payload, str):
        truncated = len(payload) > max_len
        return {"kind": "string", "preview": payload[:max_len] + ("…" if truncated else ""), "truncated": truncated}
    text = str(payload)
    truncated = len(text) > max_len
    return {"kind": "other", "preview": text[:max_len] + ("…" if truncated else ""), "truncated": truncated}


def classification_view(vector: Any) -> dict[str, Any]:
    v = vector.model_dump(mode="json")
    primary = vector.primary_category.value
    scores = {
        "relational": round(vector.relational * 100, 2),
        "document": round(vector.document * 100, 2),
        "graph": round(vector.graph * 100, 2),
        "blob": round(vector.blob * 100, 2),
        "vector": round(vector.vector * 100, 2),
    }
    return {
        **v,
        "primary_category": primary,
        "scores_percent": scores,
        "is_mixed": vector.is_mixed,
        "interpretation": (
            f"Primary: **{primary}** ({vector.confidence:.0%} confidence). "
            f"Dominant signal from layer `{vector.dominant_layer}`. "
            + (
                "Data shows mixed signals across categories — router may fan out when backends exist."
                if vector.is_mixed
                else "Single dominant category."
            )
        ),
    }


def routing_view(routing: Any) -> dict[str, Any]:
    return {
        "record_id": str(routing.record_id),
        "classification_summary": routing.classification_summary,
        "decided_at": routing.decided_at.isoformat(),
        "targets": [t.model_dump(mode="json") for t in routing.targets],
        "target_backend_ids": [t.backend_id for t in routing.targets],
        "interpretation": (
            "No storage target resolved — embedded topology may lack a backend for this category "
            "(e.g. document/graph without an adapter)."
            if not routing.targets
            else f"Routed to {len(routing.targets)} backend(s): {', '.join(t.backend_id for t in routing.targets)}."
        ),
    }


def ingest_result_view(result: IngestResult) -> dict[str, Any]:
    return {
        "record_id": str(result.record_id),
        "audit_sequence_after_ingest": result.audit_sequence,
        "backend_ids_written": result.backend_ids,
        "interpretation": (
            "Persisted to DuckDB / blob store as shown — each write is audited."
            if result.backend_ids
            else "Nothing written to disk yet (no matching backend or write staged for retry)."
        ),
    }


def audit_event_public(event: AuditEvent) -> dict[str, Any]:
    d = event.model_dump(mode="json")
    for key in ("previous_hash", "event_hash"):
        if d.get(key) and isinstance(d[key], str) and len(d[key]) > 20:
            d[key + "_short"] = d[key][:16] + "…"
    return d


async def audit_events_for_record(
    kernel: AmeobaKernel,
    *,
    record_id: uuid.UUID,
    scan_limit: int = 400,
    max_events: int = 25,
) -> list[dict[str, Any]]:
    """Scan the latest ``scan_limit`` audit rows for events tied to this record."""
    if kernel.audit_ledger is None:
        return []
    tip = kernel.audit_ledger.sequence
    start_after = max(0, tip - scan_limit)
    out: list[dict[str, Any]] = []
    async for event in kernel.audit_ledger.tail(
        after_sequence=start_after,
        limit=scan_limit,
        tenant_id=None,
    ):
        if event.record_id == record_id:
            out.append(audit_event_public(event))
            if len(out) >= max_events:
                break
    return out
