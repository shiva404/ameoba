"""POST /v1/query — federated SQL query endpoint."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel, Field

from ameoba.api.http.dependencies import AgentIdDep, KernelDep

router = APIRouter(prefix="/v1/query", tags=["query"])


class QueryRequest(BaseModel):
    sql: str = Field(description="Federated SQL query", max_length=64_000)
    tenant_id: str = Field(default="default")
    max_rows: int = Field(default=1000, ge=1, le=100_000)


class QueryResponse(BaseModel):
    columns: list[str]
    rows: list[list[Any]]
    row_count: int
    backend_ids_used: list[str]
    execution_ms: float
    truncated: bool


@router.post("", response_model=QueryResponse)
async def execute_query(
    body: QueryRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> QueryResponse:
    """Execute a federated SQL query against registered backends.

    Example::

        POST /v1/query
        {"sql": "SELECT * FROM events LIMIT 20"}
    """
    try:
        result = await kernel.query(
            body.sql,
            tenant_id=body.tenant_id,
            agent_id=agent_id,
        )
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Query failed: {exc}",
        ) from exc

    rows = result.rows[: body.max_rows]
    truncated = len(result.rows) > body.max_rows

    return QueryResponse(
        columns=result.columns,
        rows=rows,
        row_count=len(rows),
        backend_ids_used=result.backend_ids_used,
        execution_ms=round(result.execution_ms, 2),
        truncated=truncated,
    )
