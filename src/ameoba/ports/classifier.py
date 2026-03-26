"""ClassifierPlugin protocol — the contract every classifier layer must satisfy."""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

from ameoba.domain.record import ClassificationVector


@runtime_checkable
class ClassifierPlugin(Protocol):
    """A single classifier in the cascade pipeline.

    Implementations are registered with the plugin registry and called in
    priority order.  Each plugin may:
    - Return a full ``ClassificationVector`` (early exit if confidence is high).
    - Return ``None`` to pass to the next layer.

    The pipeline merges results from same-priority plugins via soft voting.
    """

    # Lower number = runs first (binary detector is priority 10).
    priority: int

    # Human-readable name for logging and audit.
    name: str

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        """Synchronous classification.

        Args:
            data:    The raw payload (bytes, dict, list, str, etc.)
            context: Shared context dict — classifiers may read/write hints here.
                     Keys used by convention:
                       ``byte_budget_remaining``: int — bytes still available for inspection
                       ``content_type``: str — MIME type hint from producer
                       ``collection``: str — destination collection name

        Returns:
            A ``ClassificationVector`` if this plugin can produce a result, else ``None``.
        """
        ...
