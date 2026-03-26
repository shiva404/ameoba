"""Structlog configuration.

Call ``configure_logging()`` once at startup (in the FastAPI lifespan or CLI
entry point).  After that, every module uses::

    import structlog
    logger = structlog.get_logger(__name__)
"""

from __future__ import annotations

import logging
import sys
from typing import Literal

import structlog


def configure_logging(
    level: str = "INFO",
    fmt: Literal["json", "pretty"] = "pretty",
) -> None:
    """Configure structlog for the whole process.

    Args:
        level: Standard logging level string (DEBUG / INFO / WARNING / ERROR).
        fmt:   ``"json"`` emits newline-delimited JSON (production);
               ``"pretty"`` emits coloured human-readable output (dev).
    """
    log_level = getattr(logging, level.upper(), logging.INFO)

    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
        structlog.processors.StackInfoRenderer(),
    ]

    if fmt == "json":
        renderer: structlog.types.Processor = structlog.processors.JSONRenderer()
    else:
        renderer = structlog.dev.ConsoleRenderer(colors=sys.stderr.isatty())

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )

    # Also configure standard library logging so third-party libs are captured.
    formatter = structlog.stdlib.ProcessorFormatter(
        processors=[
            structlog.stdlib.ExtraAdder(),
            structlog.stdlib.ProcessorFormatter.remove_processors_meta,
            *shared_processors,
            renderer,
        ],
    )
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(formatter)

    root_logger = logging.getLogger()
    root_logger.handlers = [handler]
    root_logger.setLevel(log_level)

    # Suppress noisy third-party loggers in non-debug modes
    if log_level > logging.DEBUG:
        for noisy in ("uvicorn.access", "asyncio", "duckdb"):
            logging.getLogger(noisy).setLevel(logging.WARNING)
