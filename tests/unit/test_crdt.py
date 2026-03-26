"""Unit tests for CRDT data structures."""

from __future__ import annotations

import time

import pytest

from ameoba.crdt.g_set import GSet
from ameoba.crdt.hlc import HLCTimestamp, HybridLogicalClock
from ameoba.crdt.lww_register import LWWRegister
from ameoba.crdt.or_set import ORSet


# ---------------------------------------------------------------------------
# GSet — grow-only set
# ---------------------------------------------------------------------------


def test_gset_add_and_contains():
    s = GSet[str]()
    s.add("a")
    s.add("b")
    assert "a" in s
    assert "b" in s
    assert "c" not in s


def test_gset_merge_is_union():
    a = GSet[int]()
    a.add(1)
    a.add(2)

    b = GSet[int]()
    b.add(2)
    b.add(3)

    merged = a.merge(b)
    assert set(merged.to_set()) == {1, 2, 3}


def test_gset_merge_idempotent():
    s = GSet[str]()
    s.add("x")
    merged = s.merge(s)
    assert set(merged.to_set()) == {"x"}


def test_gset_merge_commutative():
    a = GSet[str]()
    a.add("a")
    b = GSet[str]()
    b.add("b")

    ab = a.merge(b)
    ba = b.merge(a)
    assert set(ab.to_set()) == set(ba.to_set())


def test_gset_to_set_returns_frozenset():
    s = GSet[int]()
    s.add(1)
    result = s.to_set()
    assert isinstance(result, frozenset)


def test_gset_merge_in_place():
    a = GSet[str]()
    a.add("hello")
    b = GSet[str]()
    b.add("world")
    a.merge_in_place(b)
    assert "hello" in a
    assert "world" in a


# ---------------------------------------------------------------------------
# ORSet — observed-remove set
# ---------------------------------------------------------------------------


def test_orset_add_and_contains():
    s = ORSet[str]()
    s.add("foo")
    assert "foo" in s
    assert "bar" not in s


def test_orset_remove_after_add():
    s = ORSet[str]()
    s.add("foo")
    s.remove("foo")
    assert "foo" not in s


def test_orset_remove_nonexistent_is_noop():
    s = ORSet[str]()
    s.remove("not_there")  # should not raise
    assert "not_there" not in s


def test_orset_merge_add_wins_over_concurrent_remove():
    """If two replicas concurrently add and remove the same element,
    the add wins (OR-Set semantics)."""
    replica_a = ORSet[str]()
    replica_b = ORSet[str]()

    # A adds "x"
    replica_a.add("x")

    # B removes "x" but hasn't seen A's add (no shared tag)
    replica_b.remove("x")

    # After merge, "x" is still present (A's add wins)
    merged = replica_a.merge(replica_b)
    assert "x" in merged


def test_orset_merge_commutative():
    a = ORSet[str]()
    a.add("a")
    b = ORSet[str]()
    b.add("b")
    ab = a.merge(b)
    ba = b.merge(a)
    assert set(ab.elements()) == set(ba.elements())


def test_orset_merge_idempotent():
    s = ORSet[str]()
    s.add("x")
    merged = s.merge(s)
    assert "x" in merged


# ---------------------------------------------------------------------------
# LWWRegister — last-write-wins register
# ---------------------------------------------------------------------------


def test_lww_register_set_and_get():
    reg = LWWRegister[str]()
    reg.set("hello", timestamp=100, node_id="n1")
    assert reg.value == "hello"
    assert reg.timestamp == 100


def test_lww_register_later_timestamp_wins():
    reg = LWWRegister[int]()
    reg.set(1, timestamp=100, node_id="n1")
    reg.set(2, timestamp=200, node_id="n2")
    assert reg.value == 2


def test_lww_register_earlier_timestamp_ignored():
    reg = LWWRegister[str]()
    reg.set("latest", timestamp=200, node_id="n1")
    reg.set("old", timestamp=100, node_id="n2")
    assert reg.value == "latest"


def test_lww_register_tie_broken_by_node_id():
    reg = LWWRegister[str]()
    reg.set("a", timestamp=100, node_id="node-a")
    reg.set("z", timestamp=100, node_id="node-z")
    # Higher node_id wins on tie
    assert reg.value == "z"


def test_lww_register_merge():
    a = LWWRegister[int]()
    a.set(1, timestamp=100, node_id="n1")

    b = LWWRegister[int]()
    b.set(2, timestamp=200, node_id="n2")

    merged = a.merge(b)
    assert merged.value == 2


def test_lww_register_unset_is_none():
    reg = LWWRegister[str]()
    assert reg.value is None


# ---------------------------------------------------------------------------
# HLC — hybrid logical clock
# ---------------------------------------------------------------------------


def test_hlc_tick_advances_counter_or_wall():
    clock = HybridLogicalClock(node_id="n1")
    t1 = clock.tick()
    t2 = clock.tick()
    assert t2 > t1


def test_hlc_update_from_future_message():
    sender = HybridLogicalClock(node_id="sender")
    receiver = HybridLogicalClock(node_id="receiver")

    sent_ts = sender.tick()
    # Simulate time passing on sender
    sent_ts2 = HLCTimestamp(wall_ms=sent_ts.wall_ms + 5000, counter=0, node_id="sender")

    received_ts = receiver.update(sent_ts2)
    # Receiver should advance at least to sender's wall time
    assert received_ts.wall_ms >= sent_ts2.wall_ms


def test_hlc_timestamp_ordering():
    t1 = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
    t2 = HLCTimestamp(wall_ms=1000, counter=1, node_id="n1")
    t3 = HLCTimestamp(wall_ms=2000, counter=0, node_id="n1")

    assert t1 < t2
    assert t2 < t3
    assert t1 < t3


def test_hlc_timestamp_node_id_tiebreak():
    t1 = HLCTimestamp(wall_ms=1000, counter=0, node_id="a")
    t2 = HLCTimestamp(wall_ms=1000, counter=0, node_id="b")
    assert t1 < t2


def test_hlc_timestamp_frozen():
    t = HLCTimestamp(wall_ms=1000, counter=0, node_id="n1")
    with pytest.raises(Exception):  # dataclass frozen or attrs
        t.wall_ms = 9999  # type: ignore[misc]
