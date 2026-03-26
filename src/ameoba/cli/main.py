"""Ameoba CLI root — entry point for all commands.

Usage::

    ameoba --help
    ameoba ingest data.json --collection events
    ameoba query "SELECT * FROM events LIMIT 10"
    ameoba audit verify
    ameoba serve
"""

from __future__ import annotations

import typer

from ameoba.cli.commands.audit import app as audit_app
from ameoba.cli.commands.backend import app as backend_app
from ameoba.cli.commands.ingest import app as ingest_app
from ameoba.cli.commands.query import app as query_app
from ameoba.cli.commands.serve import app as serve_app

app = typer.Typer(
    name="ameoba",
    help="Intelligent adaptive data fabric for agentic workflows",
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.add_typer(ingest_app, name="ingest")
app.add_typer(query_app, name="query")
app.add_typer(audit_app, name="audit")
app.add_typer(serve_app, name="serve")
app.add_typer(backend_app, name="backend")


@app.command("version")
def version() -> None:
    """Print the Ameoba version."""
    from ameoba import __version__
    typer.echo(f"ameoba {__version__}")


if __name__ == "__main__":
    app()
