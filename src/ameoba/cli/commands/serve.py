"""ameoba serve — start the HTTP server."""

from __future__ import annotations

import typer

app = typer.Typer(help="Start the Ameoba HTTP server")


@app.callback(invoke_without_command=True)
def serve(
    host: str = typer.Option("0.0.0.0", "--host", "-h"),
    port: int = typer.Option(8000, "--port", "-p"),
    reload: bool = typer.Option(False, "--reload", help="Auto-reload on code changes (dev only)"),
    workers: int = typer.Option(1, "--workers", "-w"),
    log_level: str = typer.Option("info", "--log-level"),
) -> None:
    """Start the Ameoba HTTP API server."""
    import uvicorn
    from ameoba.api.http.app import create_app

    typer.echo(f"Starting Ameoba on http://{host}:{port}")
    uvicorn.run(
        "ameoba.api.http.app:create_app",
        factory=True,
        host=host,
        port=port,
        reload=reload,
        workers=workers if not reload else 1,
        log_level=log_level,
    )
