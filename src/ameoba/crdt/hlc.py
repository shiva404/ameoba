"""Hybrid Logical Clock (HLC) — causal ordering for distributed events.

Combines wall-clock time with a logical counter to provide:
- Monotonically increasing timestamps (even when wall clock goes backward)
- Causal ordering of concurrent events from different nodes
- Correlation with human-readable wall-clock time

Used by CRDTs and the staging buffer to timestamp events with causal order.

Reference: Kulkarni et al. 2014 — "Logical Physical Clocks and Consistent
Snapshots in Globally Distributed Databases"
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass


@dataclass(frozen=True, order=True)
class HLCTimestamp:
    """An HLC timestamp: (wall_clock_ms, logical_counter, node_id)."""

    wall_ms: int   # Physical wall clock in milliseconds
    counter: int   # Logical counter for same-millisecond events
    node_id: str   # For deterministic tie-breaking

    def __str__(self) -> str:
        return f"{self.wall_ms}.{self.counter}@{self.node_id}"

    @property
    def as_int(self) -> int:
        """Compact integer representation (wall_ms * 1000 + counter)."""
        return self.wall_ms * 1000 + min(self.counter, 999)


class HybridLogicalClock:
    """Thread-safe Hybrid Logical Clock.

    Usage::

        hlc = HybridLogicalClock(node_id="node-1")
        ts1 = hlc.tick()                    # local event
        ts2 = hlc.update(received_ts)       # received from remote

        assert ts2 >= ts1  # Causal ordering guaranteed
    """

    def __init__(self, node_id: str) -> None:
        self.node_id = node_id
        self._wall_ms: int = 0
        self._counter: int = 0
        self._lock = threading.Lock()

    def tick(self) -> HLCTimestamp:
        """Generate a timestamp for a local event."""
        now_ms = _wall_ms()
        with self._lock:
            if now_ms > self._wall_ms:
                self._wall_ms = now_ms
                self._counter = 0
            else:
                self._counter += 1
            return HLCTimestamp(self._wall_ms, self._counter, self.node_id)

    def update(self, received: HLCTimestamp) -> HLCTimestamp:
        """Advance the clock on receiving a remote timestamp and return a new ts.

        The returned timestamp is strictly greater than both the local clock
        and the received timestamp (causal ordering).
        """
        now_ms = _wall_ms()
        with self._lock:
            max_wall = max(now_ms, received.wall_ms, self._wall_ms)

            if max_wall == received.wall_ms == self._wall_ms:
                # Same millisecond on all clocks — increment counter
                self._counter = max(self._counter, received.counter) + 1
            elif max_wall == received.wall_ms:
                # Remote clock is ahead
                self._wall_ms = received.wall_ms
                self._counter = received.counter + 1
            elif max_wall == self._wall_ms:
                # Local clock is ahead
                self._counter += 1
            else:
                # Physical clock is ahead of both
                self._wall_ms = max_wall
                self._counter = 0

            return HLCTimestamp(self._wall_ms, self._counter, self.node_id)

    @property
    def current(self) -> HLCTimestamp:
        """Read the current timestamp without advancing it."""
        with self._lock:
            return HLCTimestamp(self._wall_ms, self._counter, self.node_id)


def _wall_ms() -> int:
    return int(time.time() * 1000)
