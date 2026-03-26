"""POST /v1/ingest — data ingestion endpoint."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from ameoba.api.http.dependencies import AgentIdDep, KernelDep
from ameoba.domain.record import DataCategory, DataLifecycle, DataRecord

router = APIRouter(prefix="/v1/ingest", tags=["ingest"])


class IngestRequest(BaseModel):
    """Request body for a single record ingestion."""

    collection: str = Field(description="Target collection / logical table name")
    payload: Any = Field(description="The data payload (JSON, string, or base64 bytes)")
    content_type: str | None = Field(default=None, description="MIME type hint")
    category_hint: DataCategory | None = Field(
        default=None,
        description="Override automatic classification (use with caution)",
    )
    lifecycle: DataLifecycle = Field(default=DataLifecycle.RAW)
    tenant_id: str = Field(default="default")
    session_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


class IngestResponse(BaseModel):
    record_id: str
    category: str
    confidence: float
    backend_ids: list[str]
    audit_sequence: int


class BatchIngestRequest(BaseModel):
    records: list[IngestRequest] = Field(max_length=1000)


class BatchIngestResponse(BaseModel):
    ingested: int
    results: list[IngestResponse]
    errors: list[dict[str, Any]] = Field(default_factory=list)


@router.post("", response_model=IngestResponse, status_code=status.HTTP_201_CREATED)
async def ingest_one(
    body: IngestRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> IngestResponse:
    """Ingest a single data record."""
    record = DataRecord(
        id=uuid.uuid4(),
        collection=body.collection,
        payload=body.payload,
        content_type=body.content_type,
        category_hint=body.category_hint,
        lifecycle=body.lifecycle,
        tenant_id=body.tenant_id,
        agent_id=agent_id,
        session_id=body.session_id,
        metadata=body.metadata,
    )

    try:
        result = await kernel.ingest(record, agent_id=agent_id)
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ingestion failed: {exc}",
        ) from exc

    return IngestResponse(
        record_id=str(result.record_id),
        category=result.classification.primary_category.value,
        confidence=round(result.classification.confidence, 4),
        backend_ids=result.backend_ids,
        audit_sequence=result.audit_sequence,
    )


@router.post("/batch", response_model=BatchIngestResponse, status_code=status.HTTP_207_MULTI_STATUS)
async def ingest_batch(
    body: BatchIngestRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> BatchIngestResponse:
    """Ingest up to 1000 records in a single request.

    Returns HTTP 207 Multi-Status so clients can inspect per-record outcomes.
    Errors are collected and returned — failed records do NOT abort the batch.
    """
    responses: list[IngestResponse] = []
    errors: list[dict[str, Any]] = []

    for i, req in enumerate(body.records):
        record = DataRecord(
            id=uuid.uuid4(),
            collection=req.collection,
            payload=req.payload,
            content_type=req.content_type,
            category_hint=req.category_hint,
            lifecycle=req.lifecycle,
            tenant_id=req.tenant_id,
            agent_id=agent_id,
            session_id=req.session_id,
            metadata=req.metadata,
        )
        try:
            result = await kernel.ingest(record, agent_id=agent_id)
            responses.append(IngestResponse(
                record_id=str(result.record_id),
                category=result.classification.primary_category.value,
                confidence=round(result.classification.confidence, 4),
                backend_ids=result.backend_ids,
                audit_sequence=result.audit_sequence,
            ))
        except Exception as exc:
            errors.append({"index": i, "error": str(exc)})

    return BatchIngestResponse(
        ingested=len(responses),
        results=responses,
        errors=errors,
    )
