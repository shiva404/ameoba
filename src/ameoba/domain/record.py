"""Core DataRecord domain model.

DataRecord is the primary unit flowing through the entire system — from
ingestion through classification, routing, storage, and query.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, model_validator


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class DataCategory(str, Enum):
    """Broad storage category assigned by the classifier."""

    RELATIONAL = "relational"
    DOCUMENT = "document"
    GRAPH = "graph"
    BLOB = "blob"
    VECTOR = "vector"
    UNKNOWN = "unknown"


class DataLifecycle(str, Enum):
    """Lifecycle stage of the record within a workflow."""

    RAW = "raw"           # Unprocessed input (files, API responses, scrapes)
    INTERMEDIATE = "intermediate"  # Partially processed (extracted entities, parsed)
    FINAL = "final"       # Authoritative output (reports, decisions, summaries)


class ClassificationVector(BaseModel):
    """Probability distribution over DataCategory values.

    The classifier outputs a vector, not a single label.  Mixed data is
    decomposed into sub-records using the highest-confidence category per part.
    """

    relational: float = Field(default=0.0, ge=0.0, le=1.0)
    document: float = Field(default=0.0, ge=0.0, le=1.0)
    graph: float = Field(default=0.0, ge=0.0, le=1.0)
    blob: float = Field(default=0.0, ge=0.0, le=1.0)
    vector: float = Field(default=0.0, ge=0.0, le=1.0)

    # Metadata about how confident the classifier is overall
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    # Which layer produced the dominant signal
    dominant_layer: str = Field(default="unknown")

    @model_validator(mode="after")
    def normalise(self) -> ClassificationVector:
        total = self.relational + self.document + self.graph + self.blob + self.vector
        if total > 0:
            self.relational /= total
            self.document /= total
            self.graph /= total
            self.blob /= total
            self.vector /= total
        return self

    @property
    def primary_category(self) -> DataCategory:
        """The category with the highest probability."""
        scores = {
            DataCategory.RELATIONAL: self.relational,
            DataCategory.DOCUMENT: self.document,
            DataCategory.GRAPH: self.graph,
            DataCategory.BLOB: self.blob,
            DataCategory.VECTOR: self.vector,
        }
        best = max(scores, key=lambda k: scores[k])
        return best if scores[best] > 0 else DataCategory.UNKNOWN

    @property
    def is_mixed(self) -> bool:
        """True when more than one category has significant probability (>0.2)."""
        nonzero = sum(
            1 for v in [self.relational, self.document, self.graph, self.blob, self.vector]
            if v > 0.2
        )
        return nonzero > 1


class DataRecord(BaseModel):
    """The primary data container that flows through Ameoba.

    Every piece of data that enters the system is wrapped in a DataRecord.
    Producers can provide explicit hints (``category``, ``schema_hint``) to
    bypass classification; otherwise the pipeline infers them.
    """

    # Identity
    id: uuid.UUID = Field(default_factory=uuid.uuid4)
    parent_id: uuid.UUID | None = Field(
        default=None,
        description="Set when this record was decomposed from a parent mixed record",
    )
    collection: str = Field(
        description="Logical collection / table name this record belongs to",
    )

    # Content
    payload: Any = Field(description="The raw data payload")
    content_type: str | None = Field(
        default=None,
        description="MIME type hint from producer (e.g. application/json)",
    )

    # Producer-provided hints (bypass classification if set)
    category_hint: DataCategory | None = Field(
        default=None,
        description="If producer declares the type, trust it (confidence=1.0)",
    )
    schema_hint: dict[str, Any] | None = Field(
        default=None,
        description="Optional JSON Schema hint from producer",
    )

    # Lifecycle
    lifecycle: DataLifecycle = Field(default=DataLifecycle.RAW)

    # Classification result (populated by the kernel after ingestion)
    classification: ClassificationVector | None = Field(default=None)
    schema_version_id: uuid.UUID | None = Field(default=None)

    # Agent context
    agent_id: str | None = Field(
        default=None,
        description="Identity of the agent that produced this record",
    )
    session_id: str | None = Field(
        default=None,
        description="Workflow session this record belongs to",
    )
    tenant_id: str = Field(
        default="default",
        description="Tenant boundary for multi-tenancy",
    )

    # Timestamps
    created_at: datetime = Field(default_factory=_utcnow)
    ingested_at: datetime | None = Field(default=None)

    # Arbitrary producer metadata (not queried, just stored)
    metadata: dict[str, Any] = Field(default_factory=dict)

    model_config = {"arbitrary_types_allowed": True}
