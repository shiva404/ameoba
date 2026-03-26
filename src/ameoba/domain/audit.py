"""Audit domain models.

Every significant operation produces an AuditEvent.  Events are immutable
once created and are written to the append-only ledger.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class AuditEventKind(str, Enum):
    """Exhaustive list of operations that generate audit events."""

    # Data flow
    INGESTION = "ingestion"
    CLASSIFICATION = "classification"
    ROUTING = "routing"
    WRITE = "write"
    STAGING_ENQUEUED = "staging_enqueued"
    READ = "read"
    QUERY = "query"
    DELETE = "delete"        # Logical deletes only (cryptographic erasure for GDPR)

    # Schema
    SCHEMA_REGISTERED = "schema_registered"
    SCHEMA_DRIFT_DETECTED = "schema_drift_detected"

    # Backend lifecycle
    BACKEND_REGISTERED = "backend_registered"
    BACKEND_HEALTH_CHANGED = "backend_health_changed"
    BACKEND_PROMOTED = "backend_promoted"    # Embedded → external promotion

    # Security
    AUTH_SUCCESS = "auth_success"
    AUTH_FAILURE = "auth_failure"
    AUTHZ_DENIED = "authz_denied"
    POLICY_CHANGED = "policy_changed"

    # System
    SYSTEM_START = "system_start"
    SYSTEM_STOP = "system_stop"
    AUDIT_VERIFIED = "audit_verified"
    AUDIT_TAMPER_DETECTED = "audit_tamper_detected"


class AuditEvent(BaseModel):
    """An immutable record of a single operation.

    Fields are intentionally flat and primitive to keep the ledger schema
    stable across schema versions.
    """

    # Identity / ordering
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    sequence: int | None = Field(
        default=None,
        description="Monotonically increasing sequence number (assigned by ledger)",
    )

    # What happened
    kind: AuditEventKind
    occurred_at: datetime = Field(default_factory=_utcnow)

    # Who did it
    agent_id: str | None = None
    session_id: str | None = None
    tenant_id: str = "default"

    # What was affected
    record_id: uuid.UUID | None = None
    collection: str | None = None
    backend_id: str | None = None

    # Machine-readable detail (kept small — not a free-form blob)
    detail: dict[str, Any] = Field(default_factory=dict)

    # Merkle chain (populated by the ledger, not the producer)
    previous_hash: str | None = Field(
        default=None,
        description="Hash of the previous event in the chain (hex)",
    )
    event_hash: str | None = Field(
        default=None,
        description="SHA-256 of this event's canonical representation (hex)",
    )

    model_config = {"frozen": True}


class MerkleNode(BaseModel):
    """A node in the RFC 6962 Merkle tree over the audit log."""

    hash: str          # Hex-encoded SHA-256
    left: str | None = None   # Child node hashes (None for leaves)
    right: str | None = None
    sequence_range: tuple[int, int] | None = None  # Leaf → (seq, seq)

    model_config = {"frozen": True}
