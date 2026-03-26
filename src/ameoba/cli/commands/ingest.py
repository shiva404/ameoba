"""ameoba ingest — push data from CLI."""

from __future__ import annotations

import asyncio
import json
import sys
from pathlib import Path
from typing import Optional

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(help="Ingest data into Ameoba")


@app.callback(invoke_without_command=True)
def ingest(
    file: Optional[Path] = typer.Argument(
        default=None,
        help="Path to JSON file (or stdin if omitted)",
    ),
    collection: str = typer.Option(..., "--collection", "-c", help="Target collection name"),
    content_type: Optional[str] = typer.Option(None, "--content-type", "-t"),
    lifecycle: str = typer.Option("raw", "--lifecycle", "-l"),
    tenant_id: str = typer.Option("default", "--tenant"),
    category_hint: Optional[str] = typer.Option(
        None, "--category",
        help="Override classification: relational|document|graph|blob|vector",
    ),
) -> None:
    """Ingest a JSON file (or stdin) into Ameoba."""
    asyncio.run(_ingest_async(
        file=file,
        collection=collection,
        content_type=content_type,
        lifecycle=lifecycle,
        tenant_id=tenant_id,
        category_hint=category_hint,
    ))


async def _ingest_async(
    *,
    file: Optional[Path],
    collection: str,
    content_type: Optional[str],
    lifecycle: str,
    tenant_id: str,
    category_hint: Optional[str],
) -> None:
    from ameoba.config import settings
    from ameoba.domain.record import DataCategory, DataLifecycle, DataRecord
    from ameoba.kernel.kernel import AmeobaKernel
    from ameoba.observability.logging import configure_logging

    configure_logging(level=settings.obs.log_level, fmt="pretty")

    # Read payload
    if file:
        text = file.read_text()
    else:
        text = sys.stdin.read()

    try:
        payload = json.loads(text)
    except json.JSONDecodeError:
        payload = text  # treat as raw string

    # Build record
    cat_hint = None
    if category_hint:
        try:
            cat_hint = DataCategory(category_hint.lower())
        except ValueError:
            console.print(f"[red]Invalid category hint: {category_hint}[/red]")
            raise typer.Exit(1)

    record = DataRecord(
        collection=collection,
        payload=payload,
        content_type=content_type,
        category_hint=cat_hint,
        lifecycle=DataLifecycle(lifecycle),
        tenant_id=tenant_id,
    )

    kernel = AmeobaKernel(settings)
    await kernel.start()
    try:
        result = await kernel.ingest(record)
    finally:
        await kernel.stop()

    # Display result
    table = Table(title="Ingestion Result", show_header=True)
    table.add_column("Field", style="cyan")
    table.add_column("Value", style="green")
    table.add_row("Record ID", str(result.record_id))
    table.add_row("Category", result.classification.primary_category.value)
    table.add_row("Confidence", f"{result.classification.confidence:.2%}")
    table.add_row("Layer", result.classification.dominant_layer)
    table.add_row("Backends", ", ".join(result.backend_ids))
    table.add_row("Audit Seq", str(result.audit_sequence))
    console.print(table)
