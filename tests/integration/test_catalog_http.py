"""Integration tests for GET /v1/catalog and debug catalog UI."""

from __future__ import annotations

from typing import Any

from fastapi.testclient import TestClient

from ameoba.api.http.app import create_app


def test_get_catalog_empty_platform(test_settings: Any) -> None:
    app = create_app(test_settings)
    with TestClient(app) as client:
        r = client.get("/v1/catalog")
        assert r.status_code == 200
        body = r.json()
        assert body["tenant_id"] == "default"
        assert "collections" in body
        assert isinstance(body["collections"], list)
        assert "staging_groups" in body
        assert "staging_pending_total" in body
        assert "audit_events_by_kind" in body
        assert "backends" in body
        assert any(b["id"] == "duckdb-embedded" for b in body["backends"])


def test_get_catalog_after_populate(test_settings: Any) -> None:
    app = create_app(test_settings)
    with TestClient(app) as client:
        client.post(
            "/v1/debug/populate",
            json={"scenario": "mixed_small", "tenant_id": "default"},
        )
        r = client.get("/v1/catalog")
        assert r.status_code == 200
        body = r.json()
        assert len(body["collections"]) >= 1
        tables = {c["duckdb_table"] for c in body["collections"]}
        assert "schema_registry" not in tables
        assert "staging_buffer" not in tables


def test_debug_catalog_page_renders(test_settings: Any) -> None:
    app = create_app(test_settings)
    with TestClient(app) as client:
        r = client.get("/debug/catalog")
        assert r.status_code == 200
        assert "Data catalog" in r.text
        assert "/v1/catalog" in r.text
