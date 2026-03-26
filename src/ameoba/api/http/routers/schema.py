"""Schema registry HTTP router.

Endpoints:
    GET  /v1/schema                       List collections with registered schemas
    GET  /v1/schema/{collection}          Get latest schema for a collection
    GET  /v1/schema/{collection}/versions List all schema versions
    POST /v1/schema/{collection}/infer    Infer and register a schema from sample records
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from ameoba.api.http.dependencies import KernelDep

router = APIRouter(prefix="/v1/schema", tags=["schema"])


# ---------------------------------------------------------------------------
# Response models
# ---------------------------------------------------------------------------


class SchemaVersionResponse(BaseModel):
    collection: str
    version_number: int
    compatibility: str
    schema_json: dict[str, Any]
    registered_at: str
    record_count: int
    field_count: int
    nesting_depth: int
    key_consistency_score: float
    complexity_score: float


class CollectionListResponse(BaseModel):
    collections: list[str]


class SchemaVersionListResponse(BaseModel):
    collection: str
    versions: list[SchemaVersionResponse]


class InferRequest(BaseModel):
    records: list[dict[str, Any]] = Field(
        ...,
        description="Sample records to infer schema from",
        min_length=1,
        max_length=10_000,
    )


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------


@router.get("", response_model=CollectionListResponse)
async def list_schema_collections(kernel: KernelDep) -> CollectionListResponse:
    """List all collections that have a registered schema."""
    if kernel.schema_registry is None:
        return CollectionListResponse(collections=[])
    collections = await kernel.schema_registry.list_collections()
    return CollectionListResponse(collections=collections)


@router.get("/{collection}", response_model=SchemaVersionResponse)
async def get_latest_schema(collection: str, kernel: KernelDep) -> SchemaVersionResponse:
    """Get the latest schema version for a collection."""
    if kernel.schema_registry is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Schema registry unavailable")

    version = await kernel.schema_registry.get_latest(collection)
    if version is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"No schema found for '{collection}'")

    return _to_response(version)


@router.get("/{collection}/versions", response_model=SchemaVersionListResponse)
async def list_schema_versions(collection: str, kernel: KernelDep) -> SchemaVersionListResponse:
    """Get all schema versions for a collection (oldest first)."""
    if kernel.schema_registry is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Schema registry unavailable")

    versions = await kernel.schema_registry.list_versions(collection)
    return SchemaVersionListResponse(
        collection=collection,
        versions=[_to_response(v) for v in versions],
    )


@router.post("/{collection}/infer", response_model=SchemaVersionResponse, status_code=status.HTTP_201_CREATED)
async def infer_and_register_schema(
    collection: str,
    body: InferRequest,
    kernel: KernelDep,
) -> SchemaVersionResponse:
    """Infer a schema from sample records and register it.

    Useful for pre-registering a schema before the first ingest,
    or for explicit schema management.
    """
    if kernel.schema_registry is None:
        raise HTTPException(status_code=status.HTTP_503_SERVICE_UNAVAILABLE, detail="Schema registry unavailable")

    version = await kernel.schema_registry.register_from_records(collection, body.records)
    return _to_response(version)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _to_response(version: Any) -> SchemaVersionResponse:
    return SchemaVersionResponse(
        collection=version.collection,
        version_number=version.version_number,
        compatibility=version.compatibility.value,
        schema_json=version.json_schema,
        registered_at=version.created_at.isoformat(),
        record_count=version.record_count_at_inference,
        field_count=version.field_count,
        nesting_depth=version.nesting_depth,
        key_consistency_score=version.key_consistency_score,
        complexity_score=version.complexity_score,
    )
