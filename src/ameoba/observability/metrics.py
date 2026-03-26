"""OpenTelemetry metrics configuration.

Sets up a MeterProvider with OTLP export.  Falls back to a no-op provider
if opentelemetry-sdk is not installed.

Exposes module-level counters and histograms used across the codebase:
    - ameoba.ingest.records_total     (counter)
    - ameoba.ingest.bytes_total       (counter)
    - ameoba.query.requests_total     (counter)
    - ameoba.query.latency_ms         (histogram)
    - ameoba.audit.events_total       (counter)
    - ameoba.staging.pending          (observable gauge)

Usage::

    from ameoba.observability.metrics import get_meter, configure_metrics

    configure_metrics(service_name="ameoba", otlp_endpoint="http://localhost:4317")
    meter = get_meter(__name__)
    counter = meter.create_counter("my_counter")
    counter.add(1, attributes={"collection": "users"})
"""

from __future__ import annotations

from typing import Any

import structlog

logger = structlog.get_logger(__name__)

_meter_provider: Any = None
_metrics_module: Any = None


def configure_metrics(
    service_name: str = "ameoba",
    otlp_endpoint: str | None = None,
    export_interval_ms: int = 30_000,
) -> None:
    """Initialise the global MeterProvider.

    Args:
        service_name:       Emitted as ``service.name`` resource attribute.
        otlp_endpoint:      OTLP gRPC endpoint.  None → stdout exporter in dev.
        export_interval_ms: How often metrics are flushed (default 30 s).
    """
    global _meter_provider, _metrics_module

    try:
        from opentelemetry import metrics  # type: ignore[import]
        from opentelemetry.sdk.metrics import MeterProvider  # type: ignore[import]
        from opentelemetry.sdk.metrics.export import PeriodicExportingMetricReader  # type: ignore[import]
        from opentelemetry.sdk.resources import Resource  # type: ignore[import]

        resource = Resource.create({"service.name": service_name})

        if otlp_endpoint:
            try:
                from opentelemetry.exporter.otlp.proto.grpc.metric_exporter import (  # type: ignore[import]
                    OTLPMetricExporter,
                )
                exporter = OTLPMetricExporter(endpoint=otlp_endpoint)
                logger.info("metrics_otlp_configured", endpoint=otlp_endpoint)
            except ImportError:
                from opentelemetry.sdk.metrics.export import ConsoleMetricExporter  # type: ignore[import]
                exporter = ConsoleMetricExporter()
                logger.warning(
                    "metrics_otlp_exporter_unavailable",
                    hint="pip install opentelemetry-exporter-otlp-proto-grpc",
                )
        else:
            from opentelemetry.sdk.metrics.export import ConsoleMetricExporter  # type: ignore[import]
            exporter = ConsoleMetricExporter()
            logger.info("metrics_console_configured")

        reader = PeriodicExportingMetricReader(
            exporter, export_interval_millis=export_interval_ms
        )
        provider = MeterProvider(resource=resource, metric_readers=[reader])
        metrics.set_meter_provider(provider)
        _meter_provider = provider
        _metrics_module = metrics
        logger.info("metrics_initialised", service_name=service_name)

    except ImportError:
        logger.info(
            "metrics_unavailable",
            hint="pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc",
        )
        _meter_provider = None
        _metrics_module = None


def get_meter(name: str) -> Any:
    """Return a meter for the given module name.

    Returns a no-op meter if opentelemetry-sdk is not installed.
    """
    if _metrics_module is not None:
        return _metrics_module.get_meter(name)
    return _NoOpMeter()


def shutdown_metrics() -> None:
    """Flush and shut down the meter provider."""
    if _meter_provider is not None:
        try:
            _meter_provider.shutdown()
        except Exception:
            logger.exception("metrics_shutdown_error")


# ---------------------------------------------------------------------------
# No-op fallback
# ---------------------------------------------------------------------------


class _NoOpInstrument:
    def add(self, amount: int | float, attributes: dict | None = None) -> None:
        pass

    def record(self, amount: int | float, attributes: dict | None = None) -> None:
        pass


class _NoOpMeter:
    def create_counter(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()

    def create_histogram(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()

    def create_up_down_counter(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()

    def create_observable_gauge(self, name: str, **kwargs: Any) -> _NoOpInstrument:
        return _NoOpInstrument()
