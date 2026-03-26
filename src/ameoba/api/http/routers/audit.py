"""GET /v1/audit — audit ledger read endpoints."""

from __future__ import annotations

from fastapi import APIRouter, HTTPException, Query, status
from pydantic import BaseModel

from ameoba.api.http.dependencies import AgentIdDep, KernelDep
from ameoba.domain.audit import AuditEvent

router = APIRouter(prefix="/v1/audit", tags=["audit"])


class AuditTailResponse(BaseModel):
    events: list[AuditEvent]
    count: int
    root_hash: str


class AuditVerifyResponse(BaseModel):
    ok: bool
    detail: str
    root_hash: str
    sequence: int


@router.get("/tail", response_model=AuditTailResponse)
async def tail_audit(
    kernel: KernelDep,
    agent_id: AgentIdDep,
    after_sequence: int = Query(default=0, ge=0),
    limit: int = Query(default=50, ge=1, le=500),
    tenant_id: str = Query(default="default"),
) -> AuditTailResponse:
    """Stream recent audit events in sequence order."""
    if kernel.audit_ledger is None:
        raise HTTPException(status_code=503, detail="Audit ledger not initialised")

    events: list[AuditEvent] = []
    async for event in kernel.audit_ledger.tail(
        after_sequence=after_sequence,
        limit=limit,
        tenant_id=tenant_id if tenant_id != "default" else None,
    ):
        events.append(event)

    return AuditTailResponse(
        events=events,
        count=len(events),
        root_hash=kernel.audit_ledger.root_hash,
    )


@router.get("/verify", response_model=AuditVerifyResponse)
async def verify_audit(
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> AuditVerifyResponse:
    """Verify the integrity of the entire audit ledger.

    Performs a full chain-hash scan — use sparingly on large ledgers.
    """
    if kernel.audit_ledger is None:
        raise HTTPException(status_code=503, detail="Audit ledger not initialised")

    ok, detail = await kernel.audit_verify()
    return AuditVerifyResponse(
        ok=ok,
        detail=detail,
        root_hash=kernel.audit_ledger.root_hash,
        sequence=kernel.audit_ledger.sequence,
    )
