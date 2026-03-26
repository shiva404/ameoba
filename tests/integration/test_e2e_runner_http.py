"""Integration tests for e2e runner HTTP endpoints and debug UI."""

from __future__ import annotations

from fastapi.testclient import TestClient

from ameoba.api.http.app import create_app


def test_debug_page_renders(test_settings) -> None:  # type: ignore[no-untyped-def]
    app = create_app(test_settings)
    with TestClient(app) as client:
        response = client.get("/debug/e2e")
        assert response.status_code == 200
        assert "Ameoba E2E Debug Runner" in response.text


def test_populate_and_run_scenario(test_settings) -> None:  # type: ignore[no-untyped-def]
    app = create_app(test_settings)
    with TestClient(app) as client:
        scenarios = client.get("/v1/debug/scenarios")
        assert scenarios.status_code == 200
        assert "mixed_small" in scenarios.json()["scenarios"]

        populate = client.post(
            "/v1/debug/populate",
            json={"scenario": "mixed_small", "tenant_id": "default"},
        )
        assert populate.status_code == 200
        pop_body = populate.json()
        assert pop_body["ingested"] > 0
        assert "relational" in pop_body["categories"]

        run = client.post(
            "/v1/debug/run",
            json={"scenario": "mixed_small", "tenant_id": "default"},
        )
        assert run.status_code == 200
        run_body = run.json()
        assert run_body["scenario"] == "mixed_small"
        assert run_body["ingested"] > 0
        assert run_body["audit_ok"] is True
        assert len(run_body["query_checks"]) >= 2
