"""ameoba query — run federated SQL."""

from __future__ import annotations

import asyncio

import typer
from rich.console import Console
from rich.table import Table

console = Console()
app = typer.Typer(help="Run a federated SQL query")


@app.callback(invoke_without_command=True)
def query_cmd(
    sql: str = typer.Argument(help="SQL query to execute"),
    tenant_id: str = typer.Option("default", "--tenant"),
    max_rows: int = typer.Option(100, "--max-rows", "-n"),
    output_format: str = typer.Option("table", "--output", "-o", help="table|json|csv"),
) -> None:
    """Execute a SQL query against Ameoba's registered backends."""
    asyncio.run(_query_async(
        sql=sql,
        tenant_id=tenant_id,
        max_rows=max_rows,
        output_format=output_format,
    ))


async def _query_async(
    *,
    sql: str,
    tenant_id: str,
    max_rows: int,
    output_format: str,
) -> None:
    from ameoba.config import settings
    from ameoba.kernel.kernel import AmeobaKernel
    from ameoba.observability.logging import configure_logging

    configure_logging(level=settings.obs.log_level, fmt="pretty")

    kernel = AmeobaKernel(settings)
    await kernel.start()
    try:
        result = await kernel.query(sql, tenant_id=tenant_id)
    finally:
        await kernel.stop()

    rows = result.rows[:max_rows]

    if output_format == "json":
        import json
        data = [dict(zip(result.columns, row)) for row in rows]
        console.print_json(json.dumps(data))
    elif output_format == "csv":
        import csv
        import io
        out = io.StringIO()
        writer = csv.writer(out)
        writer.writerow(result.columns)
        writer.writerows(rows)
        console.print(out.getvalue())
    else:
        table = Table(title=f"Query Results ({len(rows)} rows, {result.execution_ms:.1f}ms)")
        for col in result.columns:
            table.add_column(col, style="cyan")
        for row in rows:
            table.add_row(*[str(v) if v is not None else "NULL" for v in row])
        console.print(table)
        if len(result.rows) > max_rows:
            console.print(f"[yellow]Results truncated to {max_rows} rows[/yellow]")
