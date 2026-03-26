"""Tests for the RFC 6962 Merkle tree implementation."""

from __future__ import annotations

import pytest

from ameoba.audit.merkle import MerkleTree, internal_hash, leaf_hash


def test_empty_tree_has_deterministic_root():
    tree = MerkleTree()
    assert len(tree.root) == 64  # 32 bytes hex


def test_single_leaf():
    tree = MerkleTree()
    h = tree.append(b"hello")
    assert tree.root == h
    assert tree.size == 1


def test_two_leaves_root_changes():
    tree = MerkleTree()
    tree.append(b"a")
    root_1 = tree.root
    tree.append(b"b")
    root_2 = tree.root
    assert root_1 != root_2


def test_deterministic_root():
    """Same leaves in same order → same root."""
    tree1 = MerkleTree()
    tree2 = MerkleTree()
    for data in [b"x", b"y", b"z"]:
        tree1.append(data)
        tree2.append(data)
    assert tree1.root == tree2.root


def test_inclusion_proof_single():
    tree = MerkleTree()
    tree.append(b"only")
    proof = tree.inclusion_proof(0)
    assert proof == []  # No siblings needed for a single leaf


def test_inclusion_proof_two_leaves():
    tree = MerkleTree()
    tree.append(b"left")
    tree.append(b"right")
    proof0 = tree.inclusion_proof(0)
    proof1 = tree.inclusion_proof(1)
    # Each proof is one sibling hash
    assert len(proof0) == 1
    assert len(proof1) == 1


def test_verify_inclusion_valid():
    tree = MerkleTree()
    for i in range(8):
        tree.append(f"event-{i}".encode())

    for idx in range(8):
        proof = tree.inclusion_proof(idx)
        data = f"event-{idx}".encode()
        assert tree.verify_inclusion(idx, data, proof), f"Proof failed for index {idx}"


def test_verify_inclusion_tampered():
    tree = MerkleTree()
    for i in range(4):
        tree.append(f"record-{i}".encode())

    proof = tree.inclusion_proof(0)
    # Use wrong data — proof should fail
    assert not tree.verify_inclusion(0, b"wrong_data", proof)


def test_inclusion_proof_out_of_range():
    tree = MerkleTree()
    tree.append(b"only one")
    with pytest.raises(IndexError):
        tree.inclusion_proof(1)


def test_leaf_hash_domain_separation():
    data = b"test"
    lh = leaf_hash(data)
    # The internal hash of two identical leaf hashes should differ from a leaf hash
    ih = internal_hash(lh, lh)
    assert lh != ih  # Domain separation prevents second-preimage


def test_root_with_odd_leaves():
    """Odd number of leaves — last leaf is duplicated internally."""
    tree = MerkleTree()
    for i in range(5):
        tree.append(f"item-{i}".encode())
    # Should not raise
    root = tree.root
    assert len(root) == 64
