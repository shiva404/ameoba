"""HTTP-facing models for the unified data catalog snapshot."""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, Field


class CatalogStagingGroup(BaseModel):
    backend_id: str
    collection: str
    pending_count: int


class CatalogBlobStats(BaseModel):
    files_scanned: int
    truncated: bool
    total_bytes: int
    sample_hashes: list[str] = Field(default_factory=list)


class CatalogAuditKindCount(BaseModel):
    kind: str
    count: int


class CatalogCollectionEntry(BaseModel):
    """One logical storage bucket: DuckDB table + optional schema-registry names."""

    duckdb_table: str
    schema_collection_names: list[str] = Field(default_factory=list)
    row_count: int = 0
    latest_schema_version: int | None = None
    inferred_category: str | None = None


class CatalogSnapshot(BaseModel):
    tenant_id: str
    collections: list[CatalogCollectionEntry]
    staging_groups: list[CatalogStagingGroup]
    staging_pending_total: int
    blobs: CatalogBlobStats | None = None
    audit_events_by_kind: list[CatalogAuditKindCount] = Field(default_factory=list)
    audit_event_total: int = 0
    backends: list[dict[str, Any]] = Field(default_factory=list)
