"""OCSF (Open Cybersecurity Schema Framework) audit event exporter.

OCSF is the emerging standard for security telemetry, backed by AWS, Splunk,
IBM, and others.  It maps Ameoba audit events to OCSF classes for ingestion
into SIEMs (Splunk, Elastic, Amazon Security Lake).

OCSF reference: https://schema.ocsf.io/
Relevant classes:
  - 3001 (Data Access Activity)   — for READ / QUERY events
  - 3002 (Data Change Activity)   — for WRITE / DELETE events
  - 6001 (Authentication)         — for AUTH_SUCCESS / AUTH_FAILURE
  - 6003 (Authorization)          — for AUTHZ_DENIED
  - 9999 (Base Event)             — catch-all for system events
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

from ameoba.domain.audit import AuditEvent, AuditEventKind

# OCSF Activity IDs
_ACTIVITY_MAP = {
    AuditEventKind.READ: {"class_uid": 3001, "activity_id": 1},
    AuditEventKind.QUERY: {"class_uid": 3001, "activity_id": 6},
    AuditEventKind.WRITE: {"class_uid": 3002, "activity_id": 1},
    AuditEventKind.DELETE: {"class_uid": 3002, "activity_id": 4},
    AuditEventKind.AUTH_SUCCESS: {"class_uid": 6001, "activity_id": 1},
    AuditEventKind.AUTH_FAILURE: {"class_uid": 6001, "activity_id": 2},
    AuditEventKind.AUTHZ_DENIED: {"class_uid": 6003, "activity_id": 2},
}

_DEFAULT_CLASS = {"class_uid": 9999, "activity_id": 99}

# OCSF Severity IDs
_SEVERITY_MAP = {
    AuditEventKind.AUTH_FAILURE: 4,      # High
    AuditEventKind.AUTHZ_DENIED: 3,      # Medium
    AuditEventKind.AUDIT_TAMPER_DETECTED: 5,  # Critical
}
_DEFAULT_SEVERITY = 1  # Informational


def to_ocsf(event: AuditEvent) -> dict[str, Any]:
    """Convert an AuditEvent to an OCSF-compliant dict.

    The output can be serialised to JSON and sent to any OCSF-aware SIEM.
    """
    class_info = _ACTIVITY_MAP.get(event.kind, _DEFAULT_CLASS)
    severity_id = _SEVERITY_MAP.get(event.kind, _DEFAULT_SEVERITY)

    ocsf: dict[str, Any] = {
        # OCSF required fields
        "class_uid": class_info["class_uid"],
        "activity_id": class_info["activity_id"],
        "category_uid": _class_to_category(class_info["class_uid"]),
        "severity_id": severity_id,
        "time": int(event.occurred_at.timestamp() * 1000),  # OCSF: epoch ms
        "message": f"Ameoba {event.kind.value} event",
        "status_id": 1,  # Success
        "type_uid": class_info["class_uid"] * 100 + class_info["activity_id"],

        # Ameoba-specific fields mapped to OCSF extensions
        "metadata": {
            "version": "1.3.0",
            "product": {
                "name": "Ameoba",
                "vendor_name": "Ameoba",
                "version": "0.1.0",
            },
            "log_name": "ameoba_audit",
        },

        # Actor (the agent that performed the action)
        "actor": {
            "user": {
                "uid": event.agent_id or "system",
                "type_id": 0,  # Unknown type
            },
            "session": {
                "uid": event.session_id,
            } if event.session_id else None,
        },

        # Unmapped/extension fields
        "unmapped": {
            "ameoba_event_id": str(event.id),
            "ameoba_sequence": event.sequence,
            "ameoba_tenant_id": event.tenant_id,
            "ameoba_record_id": str(event.record_id) if event.record_id else None,
            "ameoba_collection": event.collection,
            "ameoba_backend_id": event.backend_id,
            "ameoba_event_hash": event.event_hash,
            "ameoba_detail": event.detail,
        },
    }

    # Add resource info for data events
    if event.collection or event.backend_id:
        ocsf["resources"] = [{
            "name": event.collection or event.backend_id,
            "type": "database_table" if event.collection else "storage_backend",
            "uid": event.backend_id,
        }]

    return _clean(ocsf)


def to_ocsf_jsonl(events: list[AuditEvent]) -> str:
    """Convert a list of events to OCSF JSONL (one JSON object per line)."""
    return "\n".join(json.dumps(to_ocsf(e), default=str) for e in events)


def _class_to_category(class_uid: int) -> int:
    """Map OCSF class UID to category UID."""
    if 3000 <= class_uid < 4000:
        return 3  # Identity & Access Management
    if 6000 <= class_uid < 7000:
        return 3  # Identity & Access Management
    return 0  # Uncategorized


def _clean(d: dict[str, Any]) -> dict[str, Any]:
    """Remove None values from a dict (OCSF requires no null fields)."""
    return {k: (_clean(v) if isinstance(v, dict) else v)
            for k, v in d.items() if v is not None}
