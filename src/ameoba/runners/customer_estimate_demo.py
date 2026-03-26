"""Demo: customer records, schema evolution, estimates, ambiguous lookup buffering.

Narrative for UI/docs:
1. Persist customers (v1 shape → evolved shape with extra columns).
2. Resolve a customer by email or name via SQL; attach estimates by stable ``customer_id``.
3. When lookup by name matches multiple rows (ambiguous join / FK target), buffer the intent
   in ``demo_buffered_estimates`` and commit later once ``customer_id`` is known.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

from ameoba.domain.record import DataRecord
from ameoba.kernel.kernel import AmeobaKernel

C_CUSTOMERS = "demo_customers"
C_ESTIMATES = "demo_estimates"
C_BUFFER = "demo_buffered_estimates"


def _sql_str(s: str) -> str:
    return s.replace("'", "''")


def _row_dict(columns: list[str], row: list[Any]) -> dict[str, Any]:
    return {columns[i]: row[i] for i in range(len(columns))}


async def _query(
    kernel: AmeobaKernel,
    sql: str,
    *,
    tenant_id: str,
    agent_id: str | None,
) -> Any:
    return await kernel.query(sql, tenant_id=tenant_id, agent_id=agent_id)


async def find_customers(
    kernel: AmeobaKernel,
    *,
    tenant_id: str,
    agent_id: str | None,
    email: str | None = None,
    name: str | None = None,
) -> Any:
    """Return QueryResult rows for customers matching optional email and/or name (AND)."""
    parts = [f"\"_tenant_id\" = '{_sql_str(tenant_id)}'"]
    if email is not None:
        parts.append(f"lower(email) = lower('{_sql_str(email)}')")
    if name is not None:
        parts.append(f"lower(name) = lower('{_sql_str(name)}')")
    where = " AND ".join(parts)
    sql = f"SELECT customer_id, email, name, phone, segment FROM {C_CUSTOMERS} WHERE {where}"
    return await _query(kernel, sql, tenant_id=tenant_id, agent_id=agent_id)


async def run_customer_estimate_demo(
    kernel: AmeobaKernel,
    *,
    tenant_id: str = "default",
    agent_id: str | None = "customer-estimate-demo",
) -> dict[str, Any]:
    """Execute the full scripted demo and return a step-by-step trace."""
    steps: list[dict[str, Any]] = []

    # --- v1 customer (minimal columns — creates table)
    r1 = await kernel.ingest(
        DataRecord(
            collection=C_CUSTOMERS,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "customer_id": "cust-alice-1",
                "email": "alice@example.com",
                "name": "Alice Example",
            },
        ),
        agent_id=agent_id,
    )
    steps.append(
        {
            "step": "customer_v1_minimal_shape",
            "detail": "First persist in this collection: core columns only (table created).",
            "record_id": str(r1.record_id),
        }
    )

    # --- schema evolution: next customer carries new fields → ALTER ADD COLUMN
    r2 = await kernel.ingest(
        DataRecord(
            collection=C_CUSTOMERS,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "customer_id": "cust-bob-1",
                "email": "bob@example.com",
                "name": "Bob Builder",
                "phone": "+1-555-0100",
                "segment": "enterprise",
            },
        ),
        agent_id=agent_id,
    )
    steps.append(
        {
            "step": "customer_schema_evolution",
            "detail": "New payload keys (phone, segment) add columns; earlier rows stay valid with NULLs.",
            "record_id": str(r2.record_id),
        }
    )

    # --- Jane (unique email) for safe FK + second Jane same display name (ambiguous by name)
    r3a = await kernel.ingest(
        DataRecord(
            collection=C_CUSTOMERS,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "customer_id": "cust-jane-1",
                "email": "jane@example.com",
                "name": "Jane Doe",
                "phone": "+1-555-0110",
                "segment": "enterprise",
            },
        ),
        agent_id=agent_id,
    )
    r3b = await kernel.ingest(
        DataRecord(
            collection=C_CUSTOMERS,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "customer_id": "cust-jane-2",
                "email": "jane.d@other.com",
                "name": "Jane Doe",
                "phone": "+1-555-0199",
                "segment": "smb",
            },
        ),
        agent_id=agent_id,
    )
    steps.append(
        {
            "step": "customers_for_estimate_demo",
            "detail": "jane@example.com is unique; two rows share display name Jane Doe.",
            "record_ids": [str(r3a.record_id), str(r3b.record_id)],
        }
    )

    # --- resolve by email → stable id
    q_email = await find_customers(
        kernel,
        tenant_id=tenant_id,
        agent_id=agent_id,
        email="jane@example.com",
        name=None,
    )
    jane_rows = q_email.rows
    if not jane_rows:
        raise RuntimeError("demo: expected jane@example.com to exist")
    jane = _row_dict(q_email.columns, jane_rows[0])
    steps.append(
        {
            "step": "resolve_customer_by_email",
            "detail": "Single match — safe to attach FK.",
            "customer": jane,
        }
    )

    # --- estimate by known customer_id
    est1_id = str(uuid.uuid4())
    r4 = await kernel.ingest(
        DataRecord(
            collection=C_ESTIMATES,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "estimate_id": est1_id,
                "customer_id": jane["customer_id"],
                "amount": 1250.0,
                "currency": "USD",
                "title": "Phase 1 implementation",
            },
        ),
        agent_id=agent_id,
    )
    steps.append(
        {
            "step": "estimate_persisted_for_resolved_customer_id",
            "estimate_id": est1_id,
            "customer_id": jane["customer_id"],
            "record_id": str(r4.record_id),
        }
    )

    # --- ambiguous: estimate by name only → buffer
    q_name = await find_customers(
        kernel,
        tenant_id=tenant_id,
        agent_id=agent_id,
        email=None,
        name="Jane Doe",
    )
    ambiguous = [_row_dict(q_name.columns, row) for row in q_name.rows]
    intent_id: str | None = None
    buffered = False
    if len(ambiguous) > 1:
        intent_id = str(uuid.uuid4())
        r5 = await kernel.ingest(
            DataRecord(
                collection=C_BUFFER,
                tenant_id=tenant_id,
                agent_id=agent_id,
                payload={
                    "intent_id": intent_id,
                    "status": "pending",
                    "reason": "ambiguous_customer_name",
                    "lookup_name": "Jane Doe",
                    "lookup_email": None,
                    "candidate_customer_ids_json": json.dumps([c["customer_id"] for c in ambiguous]),
                    "amount": 499.0,
                    "currency": "USD",
                    "title": "Follow-up estimate (needs disambiguation)",
                },
            ),
            agent_id=agent_id,
        )
        buffered = True
        steps.append(
            {
                "step": "buffered_estimate_intent_ambiguous_name",
                "detail": (
                    "Multiple customers share the name — intent stored in demo_buffered_estimates "
                    "until customer_id is chosen."
                ),
                "intent_id": intent_id,
                "candidates": ambiguous,
                "record_id": str(r5.record_id),
            }
        )
    else:
        steps.append(
            {
                "step": "buffered_estimate_intent_ambiguous_name",
                "skipped": True,
                "detail": "Expected >1 Jane Doe row for this demo.",
            }
        )

    # --- safe join: estimates ⟕ customers on customer_id + tenant
    join_sql = f"""
    SELECT e.estimate_id, e.amount, e.currency, c.customer_id, c.email, c.name
    FROM {C_ESTIMATES} e
    INNER JOIN {C_CUSTOMERS} c
      ON e.customer_id = c.customer_id AND e.\"_tenant_id\" = c.\"_tenant_id\"
    WHERE e.\"_tenant_id\" = '{_sql_str(tenant_id)}'
    """
    join_ok: dict[str, Any] = {}
    try:
        jr = await _query(kernel, join_sql, tenant_id=tenant_id, agent_id=agent_id)
        join_ok = {
            "ok": True,
            "row_count": jr.row_count,
            "sample": [_row_dict(jr.columns, r) for r in jr.rows[:5]],
        }
    except Exception as exc:
        join_ok = {"ok": False, "error": str(exc)}
    steps.append(
        {
            "step": "join_customers_estimates_on_customer_id",
            "detail": "Unambiguous FK join — one row per estimate/customer pair.",
            "result": join_ok,
        }
    )

    # --- risky pattern: join only on name would multiply rows / be ambiguous
    risky_sql = f"""
    SELECT e.estimate_id, e.customer_id AS estimate_customer_id, c.customer_id AS row_customer_id
    FROM {C_ESTIMATES} e
    INNER JOIN {C_CUSTOMERS} c ON lower(c.name) = lower('{_sql_str("Jane Doe")}')
       AND e.\"_tenant_id\" = c.\"_tenant_id\"
    WHERE e.\"_tenant_id\" = '{_sql_str(tenant_id)}'
    """
    risky: dict[str, Any] = {}
    try:
        rr = await _query(kernel, risky_sql, tenant_id=tenant_id, agent_id=agent_id)
        multi = len({tuple(row) for row in rr.rows}) != len(rr.rows)
        risky = {
            "ok": True,
            "row_count": rr.row_count,
            "note": (
                "Join without tying to the estimate's customer_id can duplicate or mis-associate "
                "rows when names collide — prefer resolving customer_id first (or buffer)."
            ),
            "possibly_duplicated_semantics": rr.row_count > 1 or multi,
            "sample": [_row_dict(rr.columns, r) for r in rr.rows[:8]],
        }
    except Exception as exc:
        risky = {"ok": False, "error": str(exc)}
    steps.append(
        {
            "step": "illustrate_risky_name_only_join",
            "detail": "Contrasts with FK-safe join; wide result indicates ambiguity pressure.",
            "result": risky,
        }
    )

    return {
        "collections": {
            "customers": C_CUSTOMERS,
            "estimates": C_ESTIMATES,
            "buffered_intents": C_BUFFER,
        },
        "tenant_id": tenant_id,
        "buffered_pending_intent_id": intent_id if buffered else None,
        "steps": steps,
    }


async def commit_buffered_estimate_intent(
    kernel: AmeobaKernel,
    *,
    tenant_id: str,
    agent_id: str | None,
    intent_id: str,
    resolved_customer_id: str,
) -> dict[str, Any]:
    """Load a buffered intent by ``intent_id`` and persist a real estimate."""
    try:
        dup = (
            f"SELECT estimate_id FROM {C_ESTIMATES} WHERE source_intent_id = '{_sql_str(intent_id)}' "
            f"AND \"_tenant_id\" = '{_sql_str(tenant_id)}' LIMIT 1"
        )
        dup_res = await _query(kernel, dup, tenant_id=tenant_id, agent_id=agent_id)
        if dup_res.rows:
            return {
                "ok": True,
                "idempotent": True,
                "intent_id": intent_id,
                "estimate_id": dup_res.rows[0][0],
            }
    except Exception:
        pass  # column may not exist yet on first commit in a fresh DB

    sql = (
        f"SELECT intent_id, amount, currency, title, reason, status "
        f"FROM {C_BUFFER} WHERE intent_id = '{_sql_str(intent_id)}' "
        f"AND \"_tenant_id\" = '{_sql_str(tenant_id)}' ORDER BY \"_ingested_at\" DESC LIMIT 1"
    )
    res = await _query(kernel, sql, tenant_id=tenant_id, agent_id=agent_id)
    if not res.rows:
        return {"ok": False, "error": "buffered intent not found", "intent_id": intent_id}
    buf = _row_dict(res.columns, res.rows[0])
    if buf.get("status") != "pending":
        return {
            "ok": False,
            "error": "intent is not pending",
            "intent_id": intent_id,
            "row": buf,
        }

    new_estimate_id = str(uuid.uuid4())
    ingest_res = await kernel.ingest(
        DataRecord(
            collection=C_ESTIMATES,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "estimate_id": new_estimate_id,
                "customer_id": resolved_customer_id,
                "amount": buf.get("amount"),
                "currency": buf.get("currency"),
                "title": buf.get("title"),
                "source_intent_id": intent_id,
            },
        ),
        agent_id=agent_id,
    )

    await kernel.ingest(
        DataRecord(
            collection=C_BUFFER,
            tenant_id=tenant_id,
            agent_id=agent_id,
            payload={
                "intent_id": str(uuid.uuid4()),
                "supersedes_intent_id": intent_id,
                "status": "committed",
                "resolved_customer_id": resolved_customer_id,
                "estimate_id": new_estimate_id,
                "note": "Resolution record for prior pending intent",
            },
        ),
        agent_id=agent_id,
    )

    return {
        "ok": True,
        "intent_id": intent_id,
        "resolved_customer_id": resolved_customer_id,
        "estimate_id": new_estimate_id,
        "ingest_record_id": str(ingest_res.record_id),
    }
