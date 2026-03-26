"""LWW-Register (Last-Write-Wins Register) CRDT.

Used for:
- Record metadata (lifecycle state, ownership)
- Topology health status

Ties are broken by the timestamp.  If timestamps are equal, the node with the
higher node_id wins (deterministic).

For wall-clock safety, pair with the HLC (Hybrid Logical Clock) from hlc.py.
"""

from __future__ import annotations

from typing import Generic, TypeVar

T = TypeVar("T")


class LWWRegister(Generic[T]):
    """Last-Write-Wins Register CRDT.

    Usage::

        reg = LWWRegister[str]()
        reg.set("active", timestamp=1000, node_id="node-a")
        reg.set("inactive", timestamp=999,  node_id="node-b")  # ignored — older
        assert reg.value == "active"
    """

    def __init__(self, initial: T | None = None, *, timestamp: int = 0, node_id: str = "") -> None:
        self._value = initial
        self._timestamp = timestamp
        self._node_id = node_id
        self._has_value = initial is not None

    @property
    def value(self) -> T | None:
        return self._value

    @property
    def timestamp(self) -> int:
        return self._timestamp

    def set(self, value: T, *, timestamp: int, node_id: str = "") -> bool:
        """Update the register if the new write is more recent.

        Tie-breaking: higher node_id wins (lexicographic).

        Returns:
            True if the value was updated.
        """
        if not self._has_value:
            self._value = value
            self._timestamp = timestamp
            self._node_id = node_id
            self._has_value = True
            return True

        if timestamp > self._timestamp:
            self._value = value
            self._timestamp = timestamp
            self._node_id = node_id
            return True

        if timestamp == self._timestamp and node_id > self._node_id:
            self._value = value
            self._node_id = node_id
            return True

        return False  # Old write — ignored

    def merge(self, other: LWWRegister[T]) -> LWWRegister[T]:
        """Return a new register that is the merge of this and other."""
        result: LWWRegister[T] = LWWRegister()
        if self._has_value:
            result.set(self._value, timestamp=self._timestamp, node_id=self._node_id)  # type: ignore[arg-type]
        if other._has_value:
            result.set(other._value, timestamp=other._timestamp, node_id=other._node_id)  # type: ignore[arg-type]
        return result

    def __repr__(self) -> str:
        return f"LWWRegister(value={self._value!r}, ts={self._timestamp}, node={self._node_id!r})"
