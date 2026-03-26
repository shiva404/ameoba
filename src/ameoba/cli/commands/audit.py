"""ameoba audit — inspect and verify the audit ledger."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(help="Audit ledger commands")


@app.command("verify")
def verify() -> None:
    """Verify the integrity of the audit ledger hash chain."""
    asyncio.run(_verify_async())


@app.command("tail")
def tail(
    limit: int = typer.Option(20, "--limit", "-n"),
    after: int = typer.Option(0, "--after", help="Show events after this sequence number"),
    tenant_id: str = typer.Option("default", "--tenant"),
) -> None:
    """Show recent audit events."""
    asyncio.run(_tail_async(limit=limit, after=after, tenant_id=tenant_id))


async def _verify_async() -> None:
    from ameoba.config import settings
    from ameoba.kernel.kernel import AmeobaKernel
    from ameoba.observability.logging import configure_logging

    configure_logging(level=settings.obs.log_level, fmt="pretty")
    kernel = AmeobaKernel(settings)
    await kernel.start()
    try:
        ok, detail = await kernel.audit_verify()
    finally:
        await kernel.stop()

    if ok:
        console.print(f"[green]✓ Audit integrity verified[/green]: {detail}")
    else:
        console.print(f"[red]✗ Audit integrity FAILED[/red]: {detail}")
        raise typer.Exit(1)


async def _tail_async(*, limit: int, after: int, tenant_id: str) -> None:
    from ameoba.config import settings
    from ameoba.kernel.kernel import AmeobaKernel
    from ameoba.observability.logging import configure_logging

    configure_logging(level=settings.obs.log_level, fmt="pretty")
    kernel = AmeobaKernel(settings)
    await kernel.start()

    events = []
    try:
        async for event in kernel.audit_ledger.tail(  # type: ignore[union-attr]
            after_sequence=after,
            limit=limit,
            tenant_id=tenant_id if tenant_id != "default" else None,
        ):
            events.append(event)
    finally:
        await kernel.stop()

    table = Table(title=f"Audit Log (last {len(events)} events)")
    table.add_column("Seq", style="dim", width=6)
    table.add_column("Kind", style="cyan")
    table.add_column("Agent", style="yellow")
    table.add_column("Collection", style="green")
    table.add_column("Record ID", style="blue", width=36)
    table.add_column("Occurred At", style="white")

    for e in events:
        table.add_row(
            str(e.sequence),
            e.kind.value,
            e.agent_id or "-",
            e.collection or "-",
            str(e.record_id)[:8] + "..." if e.record_id else "-",
            e.occurred_at.strftime("%Y-%m-%d %H:%M:%S"),
        )

    console.print(table)
