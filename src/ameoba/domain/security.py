"""Security domain models — principals, tokens, data classification labels."""

from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


class DataSensitivityLabel(str, Enum):
    """Sensitivity labels propagated to Cedar policy evaluation."""
    PUBLIC = "PUBLIC"
    INTERNAL = "INTERNAL"
    CONFIDENTIAL = "CONFIDENTIAL"
    PII = "PII"
    PHI = "PHI"
    PCI = "PCI"


class AgentIdentity(BaseModel):
    """Immutable representation of an authenticated caller."""

    agent_id: str
    tenant_id: str
    groups: list[str] = Field(default_factory=list)
    session_id: str | None = None

    # Claims from the token
    scopes: list[str] = Field(default_factory=list)
    issued_at: datetime | None = None
    expires_at: datetime | None = None

    # When this identity was delegated by another agent (RFC 8693 `act` claim)
    delegated_by: str | None = None
    delegation_depth: int = 0

    model_config = {"frozen": True}


class AuthzRequest(BaseModel):
    """Input to the policy engine's ``authorize`` method."""

    principal: AgentIdentity
    action: str   # e.g. "read", "write", "query", "admin"
    resource_type: str  # e.g. "collection", "backend", "audit"
    resource_id: str | None = None
    resource_labels: list[DataSensitivityLabel] = Field(default_factory=list)
    context: dict[str, Any] = Field(default_factory=dict)


class AuthzDecision(BaseModel):
    """Output from the policy engine."""

    allowed: bool
    reason: str = ""
    # If allowed, optional filter to inject into the query (row-level security)
    row_filter: str | None = None
    # Columns to redact (field-level security)
    redacted_columns: list[str] = Field(default_factory=list)

    model_config = {"frozen": True}
