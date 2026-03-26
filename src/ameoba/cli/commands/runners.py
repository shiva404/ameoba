"""CLI commands for e2e data population and system checks."""

from __future__ import annotations

import asyncio
import json

import typer
from rich.console import Console

from ameoba.runners.e2e import scenario_names

console = Console()

app = typer.Typer(help="Run end-to-end scenarios and data population flows")


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


@app.command("list")
def list_scenarios() -> None:
    """List all built-in e2e scenarios."""
    for name in scenario_names():
        console.print(f"- {name}")


@app.command("populate")
def populate(
    scenario: str = typer.Option("mixed_small", "--scenario"),
    tenant_id: str = typer.Option("default", "--tenant"),
) -> None:
    """Populate the system with generated scenario data."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel
        from ameoba.runners.e2e import populate_data

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            result = await populate_data(
                kernel,
                scenario=scenario,
                tenant_id=tenant_id,
                agent_id="cli-runner",
            )
            console.print_json(json.dumps(result, indent=2))
        finally:
            await kernel.stop()

    _run(_inner())


@app.command("run")
def run_e2e(
    scenario: str = typer.Option("mixed_small", "--scenario"),
    tenant_id: str = typer.Option("default", "--tenant"),
) -> None:
    """Run full scenario checks (populate + query checks + audit verify + health)."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel
        from ameoba.runners.e2e import run_scenario

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            result = await run_scenario(
                kernel,
                scenario=scenario,
                tenant_id=tenant_id,
                agent_id="cli-runner",
            )
            payload = {
                "scenario": result.scenario,
                "ingested": result.ingested,
                "query_checks": result.query_checks,
                "audit_ok": result.audit_ok,
                "audit_detail": result.audit_detail,
                "health": result.health,
            }
            console.print_json(json.dumps(payload, indent=2))
        finally:
            await kernel.stop()

    _run(_inner())
