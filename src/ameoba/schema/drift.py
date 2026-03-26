"""Schema drift detection.

Monitors incoming records against the registered schema and fires an alert
when the structure changes significantly.

Key design decision: **never auto-migrate** — only detect and alert.
Migration is always a deliberate human or operator action.
"""

from __future__ import annotations

from collections import deque
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class DriftDetector:
    """Windowed schema drift detector.

    Maintains a sliding window of recent records and re-infers the schema
    every ``window_size`` records.  If the inferred schema is incompatible
    with the registered schema, fires an alert callback.

    Usage::

        detector = DriftDetector(collection="users", window_size=100)
        detector.observe(record_payload)
        # Every 100 records, checks for drift automatically
    """

    def __init__(
        self,
        collection: str,
        *,
        window_size: int = 100,
        on_drift: object = None,  # Callable[[str, str], None] | None
    ) -> None:
        self.collection = collection
        self.window_size = window_size
        self._on_drift = on_drift
        self._window: deque[dict[str, Any]] = deque(maxlen=window_size)
        self._count_since_check = 0
        self._baseline_schema: dict[str, Any] | None = None

    def set_baseline(self, schema: dict[str, Any]) -> None:
        """Set the registered schema to compare against."""
        self._baseline_schema = schema

    def observe(self, record: Any) -> bool:
        """Feed a record into the window.

        Returns:
            True if drift was detected this observation.
        """
        if isinstance(record, dict):
            self._window.append(record)
        self._count_since_check += 1

        if self._count_since_check >= self.window_size:
            self._count_since_check = 0
            return self._check_drift()
        return False

    def _check_drift(self) -> bool:
        if not self._window or self._baseline_schema is None:
            return False

        from ameoba.schema.compatibility import check_compatibility
        from ameoba.schema.inference import infer_schema
        from ameoba.domain.schema import SchemaCompatibility

        sample = list(self._window)
        current_schema = infer_schema(sample)
        compat = check_compatibility(self._baseline_schema, current_schema)

        if compat == SchemaCompatibility.BREAKING:
            logger.warning(
                "schema_drift_detected",
                collection=self.collection,
                compatibility=compat.value,
                window_size=len(sample),
            )
            if callable(self._on_drift):
                self._on_drift(self.collection, compat.value)  # type: ignore[operator]
            return True

        return False
