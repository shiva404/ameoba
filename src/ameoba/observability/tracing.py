"""OpenTelemetry tracing configuration.

Sets up a TracerProvider with OTLP gRPC export.  Falls back to a no-op
provider if opentelemetry-sdk is not installed.

Usage::

    from ameoba.observability.tracing import configure_tracing, get_tracer

    configure_tracing(service_name="ameoba", otlp_endpoint="http://localhost:4317")
    tracer = get_tracer(__name__)

    with tracer.start_as_current_span("my_operation") as span:
        span.set_attribute("record_id", str(record.id))
"""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)

_tracer_provider: Any = None
_tracer_module: Any = None


def configure_tracing(
    service_name: str = "ameoba",
    otlp_endpoint: str | None = None,
) -> None:
    """Initialise the global TracerProvider.

    Args:
        service_name:   Emitted as ``service.name`` resource attribute.
        otlp_endpoint:  OTLP gRPC endpoint (e.g. ``http://localhost:4317``).
                        If None, traces are emitted to stdout in dev mode.
    """
    global _tracer_provider, _tracer_module

    try:
        from opentelemetry import trace  # type: ignore[import]
        from opentelemetry.sdk.resources import Resource  # type: ignore[import]
        from opentelemetry.sdk.trace import TracerProvider  # type: ignore[import]
        from opentelemetry.sdk.trace.export import BatchSpanProcessor  # type: ignore[import]

        resource = Resource.create({"service.name": service_name})
        provider = TracerProvider(resource=resource)

        if otlp_endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import (  # type: ignore[import]
                    OTLPSpanExporter,
                )
                exporter = OTLPSpanExporter(endpoint=otlp_endpoint)
                provider.add_span_processor(BatchSpanProcessor(exporter))
                logger.info("tracing_otlp_configured", endpoint=otlp_endpoint)
            except ImportError:
                logger.warning(
                    "tracing_otlp_exporter_unavailable",
                    hint="pip install opentelemetry-exporter-otlp-proto-grpc",
                )
        else:
            from opentelemetry.sdk.trace.export import ConsoleSpanExporter  # type: ignore[import]
            provider.add_span_processor(
                BatchSpanProcessor(ConsoleSpanExporter())
            )
            logger.info("tracing_console_configured")

        trace.set_tracer_provider(provider)
        _tracer_provider = provider
        _tracer_module = trace
        logger.info("tracing_initialised", service_name=service_name)

    except ImportError:
        logger.info(
            "tracing_unavailable",
            hint="pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc",
        )
        _tracer_provider = None
        _tracer_module = None


def get_tracer(name: str) -> Any:
    """Return a tracer for the given module name.

    Returns a no-op tracer if opentelemetry-sdk is not installed.
    """
    if _tracer_module is not None:
        return _tracer_module.get_tracer(name)
    return _NoOpTracer()


def shutdown_tracing() -> None:
    """Flush and shut down the tracer provider."""
    if _tracer_provider is not None:
        try:
            _tracer_provider.shutdown()
        except Exception:
            logger.exception("tracing_shutdown_error")


# ---------------------------------------------------------------------------
# No-op fallback (avoids try/except in application code)
# ---------------------------------------------------------------------------


class _NoOpSpan:
    def set_attribute(self, key: str, value: Any) -> None:
        pass

    def set_status(self, *args: Any, **kwargs: Any) -> None:
        pass

    def record_exception(self, exc: Exception) -> None:
        pass

    def __enter__(self) -> _NoOpSpan:
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class _NoOpTracer:
    def start_as_current_span(self, name: str, **kwargs: Any) -> _NoOpSpan:  # type: ignore[override]
        return _NoOpSpan()

    def start_span(self, name: str, **kwargs: Any) -> _NoOpSpan:
        return _NoOpSpan()
