"""Routing domain models — decisions made by the kernel router."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class BackendStatus(str, Enum):
    AVAILABLE = "available"
    DEGRADED = "degraded"   # Responding but slow / partial
    UNAVAILABLE = "unavailable"
    PROVISIONING = "provisioning"
    UNKNOWN = "unknown"


class BackendTier(str, Enum):
    """Embedded backends need no infrastructure; external need provisioning."""
    EMBEDDED = "embedded"
    EXTERNAL = "external"


class BackendTarget(BaseModel):
    """Identifies a specific storage backend and collection within it."""

    backend_id: str = Field(description="Unique backend identifier (e.g. 'duckdb-embedded')")
    collection: str = Field(description="Collection / table / index name within the backend")
    tier: BackendTier = BackendTier.EMBEDDED

    # Used by the staging buffer when the backend is unavailable
    is_primary: bool = True


class RoutingDecision(BaseModel):
    """The router's output for a single DataRecord.

    A record may be routed to multiple targets (e.g. data goes to Postgres
    while its embeddings go to LanceDB).
    """

    record_id: uuid.UUID
    targets: list[BackendTarget] = Field(default_factory=list)
    decided_at: datetime = Field(default_factory=_utcnow)
    classification_summary: str = Field(
        default="",
        description="Human-readable classification summary for logs",
    )

    model_config = {"frozen": True}


class BackendDescriptor(BaseModel):
    """Full description of a registered backend, stored in the topology registry."""

    id: str = Field(description="Unique stable identifier")
    display_name: str
    tier: BackendTier
    status: BackendStatus = BackendStatus.UNKNOWN

    # Which data categories this backend handles
    supported_categories: list[str] = Field(default_factory=list)

    # Connection / configuration (backend-specific, opaque to the kernel)
    config: dict[str, Any] = Field(default_factory=dict)

    registered_at: datetime = Field(default_factory=_utcnow)
    last_health_check: datetime | None = None


class StagingEntry(BaseModel):
    """A record held in the staging buffer awaiting a backend."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    record_id: uuid.UUID
    target_backend_id: str
    target_collection: str
    serialised_payload: bytes = Field(description="Msgpack / JSON serialised DataRecord")
    enqueued_at: datetime = Field(default_factory=_utcnow)
    attempt_count: int = 0
    last_attempt_at: datetime | None = None
    error_detail: str | None = None
