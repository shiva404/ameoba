"""Debug endpoints and browser UI for end-to-end scenarios."""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, Field

from ameoba.api.http.dependencies import AgentIdDep, KernelDep
from ameoba.runners.e2e import populate_data, run_scenario, scenario_names

router = APIRouter(tags=["debug"])


class PopulateRequest(BaseModel):
    scenario: str = Field(default="mixed_small")
    tenant_id: str = Field(default="default")


class RunRequest(BaseModel):
    scenario: str = Field(default="mixed_small")
    tenant_id: str = Field(default="default")


@router.get("/v1/debug/scenarios")
async def list_scenarios() -> dict[str, list[str]]:
    """Return supported e2e scenarios."""
    return {"scenarios": scenario_names()}


@router.post("/v1/debug/populate")
async def populate(
    body: PopulateRequest,
    kernel: KernelDep,
    agent_id: AgentIdDep,
) -> dict[str, Any]:
    """Populate synthetic data for a scenario."""
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
    """Run a full e2e scenario (populate + verification queries + audit check)."""
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


@router.get("/debug/e2e", response_class=HTMLResponse)
async def debug_page() -> str:
    """Simple in-browser e2e harness for testing multiple scenarios."""
    return """
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Ameoba E2E Debug Runner</title>
  <style>
    body { font-family: Inter, system-ui, Arial, sans-serif; margin: 0; background: #0d1117; color: #e6edf3; }
    .wrap { max-width: 1100px; margin: 0 auto; padding: 24px; }
    .card { background: #161b22; border: 1px solid #30363d; border-radius: 12px; padding: 16px; margin-bottom: 16px; }
    h1, h2 { margin: 0 0 12px 0; }
    .row { display: flex; gap: 12px; flex-wrap: wrap; align-items: center; }
    input, select, button {
      background: #0d1117; color: #e6edf3; border: 1px solid #30363d; border-radius: 8px;
      padding: 8px 10px; font-size: 14px;
    }
    button { cursor: pointer; }
    button.primary { background: #238636; border-color: #2ea043; }
    pre {
      background: #010409; border: 1px solid #30363d; border-radius: 8px;
      padding: 12px; overflow: auto; max-height: 420px; white-space: pre-wrap;
    }
    .muted { color: #8b949e; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Ameoba E2E Debug Runner</h1>
    <p class="muted">Populate synthetic data, run scenario checks, execute custom SQL, and inspect audit/health responses.</p>

    <div class="card">
      <h2>Scenario Controls</h2>
      <div class="row">
        <label>Scenario</label>
        <select id="scenario"></select>
        <label>Tenant</label>
        <input id="tenant" value="default" />
        <button id="populateBtn">Populate Data</button>
        <button id="runBtn" class="primary">Run Full Scenario</button>
      </div>
    </div>

    <div class="card">
      <h2>Custom Query</h2>
      <div class="row">
        <input id="sql" style="min-width: 580px;" value="SELECT * FROM users LIMIT 10" />
        <button id="queryBtn">Run Query</button>
      </div>
    </div>

    <div class="card">
      <h2>System Checks</h2>
      <div class="row">
        <button id="healthBtn">Health</button>
        <button id="auditTailBtn">Audit Tail</button>
        <button id="auditVerifyBtn">Audit Verify</button>
      </div>
    </div>

    <div class="card">
      <h2>Output</h2>
      <pre id="out">Ready.</pre>
    </div>
  </div>

  <script>
    const out = document.getElementById("out");
    const scenario = document.getElementById("scenario");
    const tenant = document.getElementById("tenant");
    const sql = document.getElementById("sql");

    function log(label, data) {
      out.textContent = label + "\\n" + JSON.stringify(data, null, 2);
    }

    async function request(path, method = "GET", body = null) {
      const opts = { method, headers: { "content-type": "application/json" } };
      if (body) opts.body = JSON.stringify(body);
      const res = await fetch(path, opts);
      const data = await res.json();
      return { status: res.status, data };
    }

    async function initScenarios() {
      const { data } = await request("/v1/debug/scenarios");
      for (const name of data.scenarios) {
        const opt = document.createElement("option");
        opt.value = name;
        opt.textContent = name;
        scenario.appendChild(opt);
      }
    }

    document.getElementById("populateBtn").onclick = async () => {
      const res = await request("/v1/debug/populate", "POST", {
        scenario: scenario.value,
        tenant_id: tenant.value
      });
      log("populate", res);
    };

    document.getElementById("runBtn").onclick = async () => {
      const res = await request("/v1/debug/run", "POST", {
        scenario: scenario.value,
        tenant_id: tenant.value
      });
      log("run", res);
    };

    document.getElementById("queryBtn").onclick = async () => {
      const res = await request("/v1/query", "POST", {
        sql: sql.value,
        tenant_id: tenant.value,
        max_rows: 200
      });
      log("query", res);
    };

    document.getElementById("healthBtn").onclick = async () => log("health", await request("/v1/health"));
    document.getElementById("auditTailBtn").onclick = async () =>
      log("audit tail", await request(`/v1/audit/tail?after_sequence=0&limit=20&tenant_id=${encodeURIComponent(tenant.value)}`));
    document.getElementById("auditVerifyBtn").onclick = async () => log("audit verify", await request("/v1/audit/verify"));

    initScenarios().catch((err) => log("init error", { error: String(err) }));
  </script>
</body>
</html>
"""
