"""End-to-end test data population and scenario execution helpers."""

from __future__ import annotations

import os
from dataclasses import dataclass
from typing import Any

from ameoba.domain.record import DataCategory, DataRecord
from ameoba.kernel.kernel import AmeobaKernel


@dataclass(frozen=True, slots=True)
class E2EScenarioResult:
    """Summary of a full scenario run."""

    scenario: str
    ingested: int
    query_checks: list[dict[str, Any]]
    audit_ok: bool
    audit_detail: str
    health: dict[str, Any]


def _mixed_small_dataset(tenant_id: str) -> list[DataRecord]:
    users = [
        DataRecord(
            collection="users",
            tenant_id=tenant_id,
            payload={"id": i, "name": f"user-{i}", "tier": "pro" if i % 2 else "free"},
        )
        for i in range(1, 11)
    ]
    orders = [
        DataRecord(
            collection="orders",
            tenant_id=tenant_id,
            payload={
                "order_id": i,
                "user_id": (i % 10) + 1,
                "amount": float(i * 13),
                "status": "paid" if i % 3 else "pending",
            },
        )
        for i in range(1, 41)
    ]
    reports = [
        DataRecord(
            collection="reports",
            tenant_id=tenant_id,
            payload={
                "title": f"report-{i}",
                "metadata": {"source": "synthetic", "index": i},
                "sections": [
                    {"heading": "summary", "score": i},
                    {"heading": "details", "values": [i, i + 1, i + 2]},
                ],
            },
        )
        for i in range(1, 8)
    ]
    blobs = [
        DataRecord(
            collection="uploads",
            tenant_id=tenant_id,
            payload=os.urandom(2048),
            content_type="application/octet-stream",
            category_hint=DataCategory.BLOB,
        )
        for _ in range(3)
    ]
    return [*users, *orders, *reports, *blobs]


def _high_volume_events_dataset(tenant_id: str) -> list[DataRecord]:
    return [
        DataRecord(
            collection="events",
            tenant_id=tenant_id,
            payload={
                "event_id": i,
                "event_type": "click" if i % 2 else "view",
                "session_id": f"s-{i % 20}",
                "user_id": f"u-{i % 50}",
                "latency_ms": float((i % 17) + 3),
            },
        )
        for i in range(1, 301)
    ]


def build_scenario_records(scenario: str, tenant_id: str = "default") -> list[DataRecord]:
    """Return generated records for a named scenario."""
    if scenario == "mixed_small":
        return _mixed_small_dataset(tenant_id)
    if scenario == "high_volume_events":
        return _high_volume_events_dataset(tenant_id)
    raise ValueError(f"Unknown scenario '{scenario}'")


def scenario_names() -> list[str]:
    """Expose known scenario names for API/UI consumers."""
    return ["mixed_small", "high_volume_events"]


async def populate_data(
    kernel: AmeobaKernel,
    *,
    scenario: str,
    tenant_id: str = "default",
    agent_id: str | None = "e2e-runner",
) -> dict[str, Any]:
    """Populate the system with scenario records through the full ingest path."""
    records = build_scenario_records(scenario, tenant_id)
    results = await kernel.ingest_batch(records, agent_id=agent_id)
    categories: dict[str, int] = {}
    for item in results:
        key = item.classification.primary_category.value
        categories[key] = categories.get(key, 0) + 1
    return {
        "scenario": scenario,
        "tenant_id": tenant_id,
        "ingested": len(results),
        "categories": categories,
    }


async def run_scenario(
    kernel: AmeobaKernel,
    *,
    scenario: str,
    tenant_id: str = "default",
    agent_id: str | None = "e2e-runner",
) -> E2EScenarioResult:
    """Populate and verify core health/query/audit behavior."""
    pop = await populate_data(
        kernel,
        scenario=scenario,
        tenant_id=tenant_id,
        agent_id=agent_id,
    )

    checks: list[dict[str, Any]] = []
    if scenario == "mixed_small":
        queries = [
            "SELECT COUNT(*) AS users_total FROM users",
            "SELECT COUNT(*) AS orders_total FROM orders WHERE amount > 0",
            "SELECT COUNT(*) AS reports_total FROM reports",
        ]
    else:
        queries = [
            "SELECT COUNT(*) AS events_total FROM events",
            "SELECT AVG(latency_ms) AS avg_latency FROM events",
            "SELECT COUNT(*) AS click_total FROM events WHERE event_type = 'click'",
        ]

    for sql in queries:
        result = await kernel.query(sql, tenant_id=tenant_id, agent_id=agent_id)
        checks.append(
            {
                "sql": sql,
                "row_count": result.row_count,
                "execution_ms": round(result.execution_ms, 2),
                "backend_ids_used": result.backend_ids_used,
                "sample_rows": result.rows[:5],
            }
        )

    audit_ok, audit_detail = await kernel.audit_verify()
    health = await kernel.health()
    return E2EScenarioResult(
        scenario=scenario,
        ingested=pop["ingested"],
        query_checks=checks,
        audit_ok=audit_ok,
        audit_detail=audit_detail,
        health=health,
    )
