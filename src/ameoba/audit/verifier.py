"""Background audit verifier.

Periodically re-validates the full audit ledger hash chain to detect
tampering or corruption early — before an auditor requests a report.

Design:
- Runs as an asyncio background task (not a thread).
- Default interval: 1 hour (configurable).
- On failure, logs a CRITICAL alert and calls the optional on_failure callback.
- Verification is idempotent and non-destructive.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Coroutine
from typing import Any

import structlog

logger = structlog.get_logger(__name__)


class AuditVerifier:
    """Periodic background verifier for the audit ledger.

    Usage::

        verifier = AuditVerifier(audit_ledger, interval_seconds=3600)
        task = asyncio.create_task(verifier.run())

        # On shutdown:
        verifier.stop()
        await task
    """

    def __init__(
        self,
        audit_ledger: Any,
        *,
        interval_seconds: float = 3600.0,
        on_failure: Callable[[str], Coroutine[Any, Any, None]] | None = None,
    ) -> None:
        self._ledger = audit_ledger
        self._interval = interval_seconds
        self._on_failure = on_failure
        self._running = False
        self._last_ok: bool | None = None
        self._last_message: str = ""
        self._check_count: int = 0

    async def run(self) -> None:
        """Run verification loop until ``stop()`` is called."""
        self._running = True
        logger.info("audit_verifier_started", interval_seconds=self._interval)

        while self._running:
            try:
                await self._verify_once()
            except Exception:
                logger.exception("audit_verifier_unexpected_error")

            # Wait for the next interval, but wake up immediately when stopped
            try:
                await asyncio.wait_for(
                    self._sleep_until_stopped(),
                    timeout=self._interval,
                )
            except asyncio.TimeoutError:
                pass  # Normal — time to run the next check

    async def _sleep_until_stopped(self) -> None:
        """Sleep loop that exits when _running becomes False."""
        while self._running:
            await asyncio.sleep(1.0)

    def stop(self) -> None:
        """Signal the verifier to stop after the current check."""
        self._running = False
        logger.info("audit_verifier_stopped")

    async def _verify_once(self) -> None:
        self._check_count += 1
        ok, message = await self._ledger.verify_integrity()
        self._last_ok = ok
        self._last_message = message

        if ok:
            logger.info(
                "audit_integrity_ok",
                check_number=self._check_count,
                sequence=self._ledger.sequence,
            )
        else:
            logger.critical(
                "audit_integrity_FAILED",
                check_number=self._check_count,
                message=message,
            )
            if self._on_failure is not None:
                try:
                    await self._on_failure(message)
                except Exception:
                    logger.exception("audit_verifier_on_failure_callback_error")

    @property
    def last_result(self) -> tuple[bool | None, str]:
        """Return the last verification result as (ok, message)."""
        return self._last_ok, self._last_message

    @property
    def check_count(self) -> int:
        return self._check_count
