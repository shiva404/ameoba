"""HTTP tests for customer / estimate relationship demo."""

from __future__ import annotations

from typing import Any

import pytest
from fastapi.testclient import TestClient

from ameoba.api.http.app import create_app
from ameoba.kernel.kernel import AmeobaKernel
from ameoba.runners.customer_estimate_demo import run_customer_estimate_demo


def test_customer_estimate_demo_http(test_settings: Any) -> None:
    app = create_app(test_settings)
    with TestClient(app) as client:
        r = client.post("/v1/debug/customer-estimate/run", json={"tenant_id": "default"})
        assert r.status_code == 200
        body = r.json()
        assert body["buffered_pending_intent_id"]
        assert any(
            s.get("step") == "buffered_estimate_intent_ambiguous_name" for s in body["steps"]
        )
        intent = body["buffered_pending_intent_id"]
        c = client.post(
            "/v1/debug/customer-estimate/commit",
            json={
                "tenant_id": "default",
                "intent_id": intent,
                "resolved_customer_id": "cust-jane-2",
            },
        )
        assert c.status_code == 200
        assert c.json().get("ok") is True


@pytest.mark.asyncio
async def test_run_customer_estimate_demo_kernel(kernel: AmeobaKernel) -> None:
    body = await run_customer_estimate_demo(kernel, tenant_id="default", agent_id="test")
    assert body["collections"]["customers"] == "demo_customers"
    assert body["buffered_pending_intent_id"]
