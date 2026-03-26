"""RFC 6962 (Certificate Transparency) Merkle tree implementation.

Uses the RFC-specified 0x00/0x01 leaf/internal-node prefixes to prevent
second-preimage attacks.  The tree is built incrementally over an ordered
sequence of audit events.

References:
    - RFC 6962 §2.1: https://www.rfc-editor.org/rfc/rfc6962#section-2.1
    - Certificate Transparency: "No Hash Confusion" via domain separation
"""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import Sequence


# RFC 6962 domain-separation prefixes
_LEAF_PREFIX = b"\x00"
_INTERNAL_PREFIX = b"\x01"


def leaf_hash(data: bytes) -> str:
    """Hash a leaf node: SHA-256(0x00 || data).

    Returns:
        Lowercase hex string.
    """
    digest = hashlib.sha256(_LEAF_PREFIX + data).hexdigest()
    return digest


def internal_hash(left: str, right: str) -> str:
    """Hash an internal node: SHA-256(0x01 || left || right).

    Args:
        left:  Hex-encoded left child hash.
        right: Hex-encoded right child hash.

    Returns:
        Lowercase hex string.
    """
    left_bytes = bytes.fromhex(left)
    right_bytes = bytes.fromhex(right)
    digest = hashlib.sha256(_INTERNAL_PREFIX + left_bytes + right_bytes).hexdigest()
    return digest


@dataclass
class MerkleTree:
    """Incremental Merkle tree over an append-only sequence of byte values.

    Leaf nodes hold the hash of each audit event's canonical bytes.
    Internal nodes are computed bottom-up.  The root hash summarises
    the entire sequence; any tampering changes the root.

    Usage::

        tree = MerkleTree()
        tree.append(event_bytes_1)
        tree.append(event_bytes_2)
        root = tree.root  # str hex hash
        proof = tree.inclusion_proof(0)
    """

    _leaves: list[str] = field(default_factory=list)

    def append(self, data: bytes) -> str:
        """Append a new leaf and return its hash."""
        h = leaf_hash(data)
        self._leaves.append(h)
        return h

    @property
    def root(self) -> str:
        """Return the current Merkle root hash.

        Returns an empty-string sentinel if no leaves have been added yet.
        """
        if not self._leaves:
            return hashlib.sha256(b"").hexdigest()  # empty-tree root
        return _compute_root(self._leaves)

    @property
    def size(self) -> int:
        return len(self._leaves)

    def inclusion_proof(self, index: int) -> list[str]:
        """Return the sibling-hash path proving leaf[index] is in the tree.

        Returns sibling hashes in leaf-to-root order.  The verifier
        reconstructs the root by:
            current = leaf_hash(data)
            for each (sibling, is_right) in proof:
                if is_right: current = internal(current, sibling)
                else:        current = internal(sibling, current)

        This follows the RFC 6962 §2.1.3 construction.

        Returns:
            List of hex-encoded sibling hashes from leaf to root.

        Raises:
            IndexError: if index is out of range.
        """
        n = len(self._leaves)
        if index < 0 or index >= n:
            raise IndexError(f"Leaf index {index} out of range [0, {n})")
        return _inclusion_proof_iterative(list(self._leaves), index)

    def verify_inclusion(self, index: int, leaf_data: bytes, proof: list[str]) -> bool:
        """Verify that leaf_data at position index produces the current root.

        The proof must have been produced by ``inclusion_proof`` — i.e. sibling
        hashes in leaf-to-root order.

        Returns:
            True if the proof is valid against the current root.
        """
        if index < 0 or index >= len(self._leaves):
            return False

        current = leaf_hash(leaf_data)
        nodes = list(self._leaves)
        i = index

        for sibling in proof:
            if i % 2 == 0:
                # We are a left child — sibling is to our right
                right = nodes[i + 1] if i + 1 < len(nodes) else nodes[i]
                current = internal_hash(current, right)
            else:
                # We are a right child — sibling is to our left
                current = internal_hash(nodes[i - 1], current)

            # Advance to the parent level
            next_nodes: list[str] = []
            for j in range(0, len(nodes), 2):
                left = nodes[j]
                right = nodes[j + 1] if j + 1 < len(nodes) else left
                next_nodes.append(internal_hash(left, right))
            nodes = next_nodes
            i //= 2

        return current == self.root


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _compute_root(leaves: Sequence[str]) -> str:
    """Bottom-up computation of the Merkle root for a fixed leaf list."""
    nodes = list(leaves)
    while len(nodes) > 1:
        next_level: list[str] = []
        for j in range(0, len(nodes), 2):
            left = nodes[j]
            right = nodes[j + 1] if j + 1 < len(nodes) else left  # duplicate last for odd
            next_level.append(internal_hash(left, right))
        nodes = next_level
    return nodes[0]


def _inclusion_proof_iterative(leaves: list[str], index: int) -> list[str]:
    """Collect sibling hashes from leaf level to root (leaf-to-root order).

    At each level:
    - If index is even, sibling is at index+1 (or self if unpaired).
    - If index is odd,  sibling is at index-1.
    Then advance: reduce to the parent level and halve the index.
    """
    proof: list[str] = []
    nodes = leaves

    while len(nodes) > 1:
        if index % 2 == 0:
            # Left child — sibling is on the right (duplicate if unpaired)
            sibling = nodes[index + 1] if index + 1 < len(nodes) else nodes[index]
        else:
            # Right child — sibling is on the left
            sibling = nodes[index - 1]

        proof.append(sibling)

        # Build the next (parent) level
        next_level: list[str] = []
        for j in range(0, len(nodes), 2):
            left = nodes[j]
            right = nodes[j + 1] if j + 1 < len(nodes) else left
            next_level.append(internal_hash(left, right))

        nodes = next_level
        index //= 2

    return proof
