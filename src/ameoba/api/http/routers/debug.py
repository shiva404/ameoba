"""Debug endpoints and investor demo UI — full pipeline visibility."""

from __future__ import annotations

import uuid
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request, status
from pydantic import BaseModel, Field

from ameoba.api.http.debug_trace import (
    audit_events_for_record,
    classification_view,
    ingest_result_view,
    payload_preview,
    routing_view,
)
from ameoba.api.http.dependencies import AgentIdDep, KernelDep
from ameoba.api.http.templating import templates
from ameoba.domain.record import DataCategory, DataRecord
from ameoba.runners.customer_estimate_demo import (
    commit_buffered_estimate_intent,
    run_customer_estimate_demo,
)
from ameoba.runners.e2e import populate_data, run_scenario, scenario_names

router = APIRouter(tags=["debug"])

_DEFAULT_DEMO_TRACE_PAYLOAD = """{
  "order_id": "ord-demo-1",
  "customer_id": "cust-42",
  "amount": 199.99,
  "currency": "USD",
  "line_items": [{"sku": "A1", "qty": 2}]
}"""


class PopulateRequest(BaseModel):
    scenario: str = Field(default="mixed_small")
    tenant_id: str = Field(default="default")


class RunRequest(BaseModel):
    scenario: str = Field(default="mixed_small")
    tenant_id: str = Field(default="default")


class CustomerEstimateDemoRequest(BaseModel):
    tenant_id: str = Field(default="default")


class CommitBufferedEstimateRequest(BaseModel):
    tenant_id: str = Field(default="default")
    intent_id: str = Field(description="intent_id from demo_buffered_estimates row")
    resolved_customer_id: str = Field(description="Chosen customer_id after disambiguation")


class TraceIngestRequest(BaseModel):
    """Mirror of ingest body — runs the real pipeline and returns a full trace."""

    collection: str = Field(description="Logical collection / table name")
    payload: Any = Field(description="JSON payload, string, or base64-friendly dict for demo")
    content_type: str | None = Field(default=None)
    category_hint: DataCategory | None = Field(default=None)
    tenant_id: str = Field(default="default")
    session_id: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)


@router.get("/v1/debug/scenarios")
async def list_scenarios() -> dict[str, list[str]]:
    return {"scenarios": scenario_names()}


@router.get("/v1/debug/snapshot")
async def platform_snapshot(kernel: KernelDep) -> dict[str, Any]:
    """Single call: health, topology, audit tip — for dashboard header."""
    health = await kernel.health()
    backends: list[dict[str, Any]] = []
    for desc, _be in kernel.topology.list_backends():
        backends.append(
            {
                "id": desc.id,
                "display_name": desc.display_name,
                "tier": desc.tier.value,
                "supported_categories": list(desc.supported_categories),
                "status": desc.status.value,
            }
        )
    audit_sequence = 0
    root_hash = ""
    if kernel.audit_ledger is not None:
        audit_sequence = kernel.audit_ledger.sequence
        root_hash = kernel.audit_ledger.root_hash
    return {
        "title": "Ameoba embedded platform snapshot",
        "health": health,
        "backends": backends,
        "audit": {
            "sequence": audit_sequence,
            "merkle_root_hash": root_hash,
            "merkle_root_short": (root_hash[:24] + "…") if len(root_hash) > 24 else root_hash,
        },
        "pipeline_stages": [
            {"step": 1, "name": "Ingest", "description": "Agent data arrives as a versioned DataRecord (collection + payload + tenant)."},
            {"step": 2, "name": "Classify", "description": "Multi-layer classifier scores relational / document / graph / blob / vector."},
            {"step": 3, "name": "Route", "description": "Topology-aware router picks storage backends per category (fan-out when mixed)."},
            {"step": 4, "name": "Persist", "description": "DuckDB for relational/OLAP, local blob store for binary; staging if backend is down."},
            {"step": 5, "name": "Audit", "description": "Each step emits tamper-evident, sequenced events with hash chain + Merkle root."},
            {"step": 6, "name": "Query", "description": "Federated SQL over registered backends with audited query execution."},
        ],
    }


@router.post("/v1/debug/trace-ingest")
async def trace_ingest(
    body: TraceIngestRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
    audit_scan_limit: int = Query(default=400, ge=50, le=2000),
) -> dict[str, Any]:
    """Run one real ingestion and return classification, routing, storage, and audit slice."""
    record = DataRecord(
        id=uuid.uuid4(),
        collection=body.collection,
        payload=body.payload,
        content_type=body.content_type,
        category_hint=body.category_hint,
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
            detail=f"Ingest failed: {exc}",
        ) from exc

    audit_trail = await audit_events_for_record(
        kernel,
        record_id=result.record_id,
        scan_limit=audit_scan_limit,
    )

    return {
        "story": (
            "This response is a live slice of the product: the same path production traffic uses "
            "(classify → route → write → audit). Below you can correlate the numeric classifier vector "
            "with router targets and the immutable audit trail for this record_id."
        ),
        "input": {
            "record_id": str(result.record_id),
            "collection": body.collection,
            "tenant_id": body.tenant_id,
            "content_type": body.content_type,
            "category_hint": body.category_hint.value if body.category_hint else None,
            "metadata": body.metadata,
            "payload": payload_preview(body.payload),
        },
        "classification": classification_view(result.classification),
        "routing": routing_view(result.routing),
        "storage": ingest_result_view(result),
        "audit_trail_for_record": audit_trail,
        "audit_event_kinds_seen": sorted({e["kind"] for e in audit_trail}),
    }


@router.get("/v1/debug/audit-for-record")
async def audit_for_record(
    kernel: KernelDep,
    record_id: uuid.UUID = Query(description="UUID of the DataRecord"),
    scan_limit: int = Query(default=400, ge=50, le=2000),
) -> dict[str, Any]:
    """Fetch recent audit events mentioning this record_id (linear scan of tail)."""
    events = await audit_events_for_record(kernel, record_id=record_id, scan_limit=scan_limit)
    return {"record_id": str(record_id), "events": events, "count": len(events)}


def _demo_page_response(request: Request) -> Any:
    return templates.TemplateResponse(
        request=request,
        name="debug/demo.html",
        context={
            "page_title": "Ameoba — Platform demo",
            "default_trace_payload": _DEFAULT_DEMO_TRACE_PAYLOAD,
        },
    )


@router.get("/debug/e2e")
async def debug_page(request: Request) -> Any:
    """Investor-ready demo: overview, pipeline trace, bulk e2e, raw JSON (Jinja2)."""
    return _demo_page_response(request)


@router.get("/debug/demo")
async def debug_demo_alias(request: Request) -> Any:
    """Alias for the same demo page."""
    return _demo_page_response(request)


@router.get("/debug/catalog")
async def debug_catalog_page(request: Request) -> Any:
    """Unified catalog: collections, staging, blobs, audit counts."""
    return templates.TemplateResponse(
        request=request,
        name="debug/catalog.html",
        context={"page_title": "Ameoba — Data catalog"},
    )


@router.post("/v1/debug/customer-estimate/run")
async def customer_estimate_demo_run(
    body: CustomerEstimateDemoRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> dict[str, Any]:
    """Demo: customers → schema evolution → estimate by email; buffer ambiguous name-based intent."""
    return await run_customer_estimate_demo(
        kernel,
        tenant_id=body.tenant_id,
        agent_id=agent_id or "customer-estimate-demo",
    )


@router.post("/v1/debug/customer-estimate/commit")
async def customer_estimate_demo_commit(
    body: CommitBufferedEstimateRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> dict[str, Any]:
    """Commit a buffered estimate intent once customer_id is resolved."""
    return await commit_buffered_estimate_intent(
        kernel,
        tenant_id=body.tenant_id,
        agent_id=agent_id or "customer-estimate-demo",
        intent_id=body.intent_id,
        resolved_customer_id=body.resolved_customer_id,
    )


@router.post("/v1/debug/populate")
async def populate(
    body: PopulateRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> dict[str, Any]:
    return await populate_data(
        kernel,
        scenario=body.scenario,
        tenant_id=body.tenant_id,
        agent_id=agent_id or "debug-api",
    )


@router.post("/v1/debug/run")
async def run(
    body: RunRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> dict[str, Any]:
    result = await run_scenario(
        kernel,
        scenario=body.scenario,
        tenant_id=body.tenant_id,
        agent_id=agent_id or "debug-api",
    )
    return {
        "scenario": result.scenario,
        "ingested": result.ingested,
        "query_checks": result.query_checks,
        "audit_ok": result.audit_ok,
        "audit_detail": result.audit_detail,
        "health": result.health,
    }
