"""GET /v1/catalog — unified snapshot of collections, staging, blobs, audit."""

from __future__ import annotations

from fastapi import APIRouter, Query

from ameoba.api.http.dependencies import KernelDep
from ameoba.domain.catalog import CatalogSnapshot

router = APIRouter(prefix="/v1/catalog", tags=["catalog"])


@router.get("", response_model=CatalogSnapshot)
async def get_catalog(
    kernel: KernelDep,
    tenant_id: str = Query(default="default"),
    blob_max_files: int = Query(default=5000, ge=1, le=100_000),
    blob_sample_limit: int = Query(default=100, ge=0, le=500),
) -> CatalogSnapshot:
    """Aggregated view of persisted entities: DuckDB tables, schemas, staging, blobs, audit."""
    return await kernel.catalog_snapshot(
        tenant_id=tenant_id,
        blob_max_files=blob_max_files,
        blob_sample_limit=blob_sample_limit,
    )
