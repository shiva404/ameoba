"""Jinja2 template environment for HTTP layer (demo pages, future admin UI)."""

from __future__ import annotations

from pathlib import Path

from starlette.templating import Jinja2Templates

_HTTP_DIR = Path(__file__).resolve().parent
TEMPLATES_DIR = _HTTP_DIR / "templates"

templates = Jinja2Templates(directory=str(TEMPLATES_DIR))
