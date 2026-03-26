"""Schema registry domain models."""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class SchemaCompatibility(str, Enum):
    """Relationship between two consecutive schema versions."""
    IDENTICAL = "identical"
    BACKWARD_COMPATIBLE = "backward_compatible"  # New fields added (additive)
    BREAKING = "breaking"                         # Fields removed or types changed
    UNKNOWN = "unknown"


class SchemaVersion(BaseModel):
    """An immutable snapshot of the inferred schema for a collection."""

    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    collection: str
    version_number: int = Field(ge=1)
    json_schema: dict[str, Any] = Field(description="JSON Schema representation")
    inferred_category: str  # DataCategory string value

    # Derived metrics used by the classifier
    field_count: int = 0
    nesting_depth: int = 0
    key_consistency_score: float = Field(default=0.0, ge=0.0, le=1.0)
    complexity_score: float = Field(default=0.0, ge=0.0, le=1.0)

    created_at: datetime = Field(default_factory=_utcnow)
    record_count_at_inference: int = 0

    # Lineage
    previous_version_id: uuid.UUID | None = None
    compatibility: SchemaCompatibility = SchemaCompatibility.UNKNOWN

    model_config = {"frozen": True}
