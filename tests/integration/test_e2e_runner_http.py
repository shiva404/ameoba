"""Integration tests for e2e runner HTTP endpoints and debug UI."""

from __future__ import annotations

from fastapi.testclient import TestClient

from ameoba.api.http.app import create_app


def test_debug_page_renders(test_settings) -> None:  # type: ignore[no-untyped-def]
    app = create_app(test_settings)
    with TestClient(app) as client:
        response = client.get("/debug/e2e")
        assert response.status_code == 200
        assert "Ameoba — data fabric demo" in response.text
        r2 = client.get("/debug/demo")
        assert r2.status_code == 200


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
        assert all(c.get("ok") is True for c in run_body["query_checks"])


def test_debug_snapshot_and_trace_ingest(test_settings) -> None:  # type: ignore[no-untyped-def]
    app = create_app(test_settings)
    with TestClient(app) as client:
        snap = client.get("/v1/debug/snapshot")
        assert snap.status_code == 200
        body = snap.json()
        assert "health" in body
        assert "backends" in body
        assert "pipeline_stages" in body
        assert any(b["id"] == "duckdb-embedded" for b in body["backends"])

        tr = client.post(
            "/v1/debug/trace-ingest",
            json={
                "collection": "trace_test",
                "tenant_id": "default",
                "payload": {"id": 1, "name": "alice", "score": 0.9},
            },
        )
        assert tr.status_code == 200
        t = tr.json()
        assert "classification" in t
        assert "routing" in t
        assert "storage" in t
        assert "audit_trail_for_record" in t
        assert t["classification"]["primary_category"] == "relational"
        rid = t["input"]["record_id"]
        ar = client.get(f"/v1/debug/audit-for-record?record_id={rid}")
        assert ar.status_code == 200
        assert ar.json()["count"] >= 1
