"""CLI commands for managing storage backends.

    ameoba backend list              — list registered backends and their status
    ameoba backend health            — run health checks on all backends
    ameoba backend flush-staging     — flush staged records back to backends
    ameoba backend pending           — show count of staged (pending) records
"""

from __future__ import annotations

import asyncio
import json

import typer
from rich.console import Console
from rich.table import Table

console = Console()

app = typer.Typer(help="Storage backend management")


def _run(coro):  # type: ignore[no-untyped-def]
    return asyncio.run(coro)


@app.command("list")
def list_backends() -> None:
    """List all registered backends and their current status."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            backends = kernel.topology.list_backends()
            if not backends:
                console.print("[yellow]No backends registered.[/yellow]")
                return

            table = Table(title="Registered Backends")
            table.add_column("ID", style="cyan")
            table.add_column("Display Name")
            table.add_column("Tier")
            table.add_column("Categories")
            table.add_column("Status")

            for desc, _ in backends:
                table.add_row(
                    desc.id,
                    desc.display_name,
                    desc.tier.value,
                    ", ".join(desc.supported_categories),
                    desc.status.value,
                )
            console.print(table)
        finally:
            await kernel.stop()

    _run(_inner())


@app.command("health")
def health_check() -> None:
    """Run health checks on all backends."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            health = await kernel.health()
            console.print_json(json.dumps(health, indent=2))
        finally:
            await kernel.stop()

    _run(_inner())


@app.command("flush-staging")
def flush_staging() -> None:
    """Attempt to flush all staged (pending) records to their target backends."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            results = await kernel.flush_staging()
            if results:
                for backend_id, count in results.items():
                    console.print(f"[green]Flushed {count} records to {backend_id}[/green]")
            else:
                console.print("[dim]No staged records to flush.[/dim]")
        finally:
            await kernel.stop()

    _run(_inner())


@app.command("pending")
def pending_count(
    backend_id: str = typer.Option(None, "--backend", help="Filter by backend ID"),
) -> None:
    """Show the number of records pending in the staging buffer."""

    async def _inner() -> None:
        from ameoba.config import settings
        from ameoba.kernel.kernel import AmeobaKernel

        kernel = AmeobaKernel(settings)
        await kernel.start()
        try:
            if kernel.staging_buffer is None:
                console.print("[yellow]Staging buffer not available.[/yellow]")
                return
            count = await kernel.staging_buffer.pending_count(backend_id)
            label = f"backend '{backend_id}'" if backend_id else "all backends"
            console.print(f"[bold]{count}[/bold] records pending for {label}")
        finally:
            await kernel.stop()

    _run(_inner())
