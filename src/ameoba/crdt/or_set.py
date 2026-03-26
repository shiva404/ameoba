"""OR-Set (Observed-Remove Set) CRDT.

Used for concurrent schema field additions.  Multiple agents can add fields
to a collection schema concurrently; all additions are preserved on merge.

Unlike a G-Set, an OR-Set supports removes — but removes only remove
elements that have been *observed* (i.e. the remove is tagged with the
unique token from the corresponding add).  This prevents the "remove wins"
vs "add wins" ambiguity.

For the schema registry use case, we use OR-Set to allow concurrent field
additions from different agents.  Conflicting field-type changes are surfaced
as separate tagged entries and resolved at read time (MV-Register semantics).
"""

from __future__ import annotations

import uuid
from typing import Generic, Hashable, Iterator, TypeVar

T = TypeVar("T", bound=Hashable)


class ORSet(Generic[T]):
    """Observed-Remove Set CRDT.

    Internally maintains a set of (element, unique_tag) pairs.
    An element is considered present if any pair with that element
    exists in the add-set and none of those tags are in the remove-set.
    """

    def __init__(self) -> None:
        # element → set of add-tags (each add gets a unique UUID)
        self._adds: dict[T, set[str]] = {}
        # set of removed tags
        self._removes: set[str] = set()

    def add(self, element: T) -> str:
        """Add an element and return its unique tag."""
        tag = str(uuid.uuid4())
        self._adds.setdefault(element, set()).add(tag)
        return tag

    def remove(self, element: T) -> None:
        """Remove all currently observed instances of element."""
        if element in self._adds:
            # Move all currently known tags to the remove-set
            self._removes |= self._adds.pop(element)

    def __contains__(self, element: object) -> bool:
        tags = self._adds.get(element, set())  # type: ignore[arg-type]
        live_tags = tags - self._removes
        return bool(live_tags)

    def __iter__(self) -> Iterator[T]:
        for element, tags in self._adds.items():
            if tags - self._removes:
                yield element

    def __len__(self) -> int:
        return sum(1 for _ in self)

    def merge(self, other: ORSet[T]) -> ORSet[T]:
        """Return a new OR-Set that is the merge of this and other."""
        result: ORSet[T] = ORSet()

        # Union of all add-tags
        all_elements = set(self._adds) | set(other._adds)
        for el in all_elements:
            result._adds[el] = (
                self._adds.get(el, set()) | other._adds.get(el, set())
            )

        # Union of all remove-tags (only removes that match known adds)
        result._removes = self._removes | other._removes

        return result

    def elements(self) -> frozenset[T]:
        """Return all currently live elements."""
        return frozenset(self)
