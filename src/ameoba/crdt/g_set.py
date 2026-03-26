"""G-Set (Grow-Only Set) CRDT.

Used for partition-tolerant audit event capture.  During a network partition,
agents collect events locally in a G-Set.  After the partition heals, G-Sets
from all nodes are merged (via union) and then sequenced into the canonical
audit log.

Properties:
- Append-only: elements can only be added, never removed.
- Merge: union of two G-Sets.
- Commutativity: merge(A, B) == merge(B, A).
- Associativity: merge(merge(A, B), C) == merge(A, merge(B, C)).
- Idempotency: merge(A, A) == A.
"""

from __future__ import annotations

from typing import Generic, Hashable, Iterator, TypeVar

T = TypeVar("T", bound=Hashable)


class GSet(Generic[T]):
    """A grow-only set CRDT.

    Usage::

        s1 = GSet[str]()
        s1.add("event-1")
        s2 = GSet[str]()
        s2.add("event-2")
        merged = s1.merge(s2)
        assert "event-1" in merged
        assert "event-2" in merged
    """

    def __init__(self, initial: set[T] | None = None) -> None:
        self._data: set[T] = set(initial or [])

    def add(self, element: T) -> None:
        """Add an element.  No-op if already present."""
        self._data.add(element)

    def __contains__(self, element: object) -> bool:
        return element in self._data

    def __iter__(self) -> Iterator[T]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def __repr__(self) -> str:
        return f"GSet({self._data!r})"

    def merge(self, other: GSet[T]) -> GSet[T]:
        """Return a new G-Set that is the union of this set and other."""
        return GSet(self._data | other._data)

    def merge_in_place(self, other: GSet[T]) -> None:
        """Merge other into this set (modifies self)."""
        self._data |= other._data

    def to_set(self) -> frozenset[T]:
        """Return a frozen copy of the underlying set."""
        return frozenset(self._data)

    @classmethod
    def from_set(cls, s: set[T]) -> GSet[T]:
        return cls(s)
