"""API key authentication — development and testing only.

Production deployments should use OAuth2 + JWT (see oauth2.py).
API keys are scoped and rate-limited; they never carry delegation chains.
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any


class APIKeyStore:
    """In-memory API key store.

    In production, back this with Redis or a database.  For the embedded
    MVP, the key list is loaded from settings at startup.

    Keys are stored as SHA-256 hashes — never in plaintext.
    """

    def __init__(self) -> None:
        # hashed_key → {agent_id, scopes, tenant_id}
        self._keys: dict[str, dict[str, Any]] = {}

    def add_key(
        self,
        raw_key: str,
        *,
        agent_id: str,
        tenant_id: str = "default",
        scopes: list[str] | None = None,
    ) -> None:
        """Register an API key (stores the hash, not the raw key)."""
        key_hash = _hash_key(raw_key)
        self._keys[key_hash] = {
            "agent_id": agent_id,
            "tenant_id": tenant_id,
            "scopes": scopes or ["read", "write"],
        }

    def validate(self, raw_key: str) -> dict[str, Any] | None:
        """Validate an API key and return its metadata, or None if invalid.

        Uses constant-time comparison to prevent timing attacks.
        """
        key_hash = _hash_key(raw_key)
        for stored_hash, metadata in self._keys.items():
            if hmac.compare_digest(stored_hash, key_hash):
                return metadata
        return None

    def load_from_list(self, keys: list[str], *, tenant_id: str = "default") -> None:
        """Bulk-load keys from a list (e.g. from settings).

        Assigns each key an auto-generated agent_id based on the key prefix.
        """
        for key in keys:
            agent_id = f"apikey-{key[:8]}"
            self.add_key(key, agent_id=agent_id, tenant_id=tenant_id)

    @staticmethod
    def generate_key(prefix: str = "amk") -> str:
        """Generate a cryptographically secure API key."""
        raw = secrets.token_urlsafe(32)
        return f"{prefix}_{raw}"


def _hash_key(raw_key: str) -> str:
    return hashlib.sha256(raw_key.encode("utf-8")).hexdigest()
