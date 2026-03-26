"""Envelope encryption — Cloud KMS master key → tenant KEK → data DEK.

Hierarchy:
    Cloud KMS Master Key
        └── Tenant KEK (one per tenant, rotated every 90 days)
                └── Collection DEK (one per collection, rotates with KEK)

GDPR cryptographic erasure: deleting the DEK makes all encrypted data
in that collection permanently unreadable without touching audit records.

This module provides a local-key implementation suitable for development.
Production deployments should swap ``LocalKeyProvider`` for a KMS-backed
provider (AWS KMS, GCP Cloud KMS, Azure Key Vault).

Dependencies: cryptography (optional)
"""

from __future__ import annotations

import base64
import os
import secrets
from typing import Protocol, runtime_checkable

_CRYPTO_AVAILABLE = False
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM  # type: ignore[import]
    _CRYPTO_AVAILABLE = True
except ImportError:
    pass


@runtime_checkable
class KeyProvider(Protocol):
    """Abstract key provider — swap for KMS in production."""

    def get_dek(self, tenant_id: str, collection: str) -> bytes:
        """Return the 32-byte Data Encryption Key for a collection."""
        ...

    def rotate_dek(self, tenant_id: str, collection: str) -> bytes:
        """Generate and store a new DEK (invalidates old encrypted data)."""
        ...

    def delete_dek(self, tenant_id: str, collection: str) -> None:
        """Cryptographic erasure: delete the DEK, rendering data unreadable."""
        ...


class LocalKeyProvider:
    """In-process key store — development only.

    Keys are ephemeral (lost on restart).  Production must use a real KMS.
    """

    def __init__(self) -> None:
        self._deks: dict[str, bytes] = {}  # (tenant_id, collection) → 32-byte key

    def _key_id(self, tenant_id: str, collection: str) -> str:
        return f"{tenant_id}::{collection}"

    def get_dek(self, tenant_id: str, collection: str) -> bytes:
        key_id = self._key_id(tenant_id, collection)
        if key_id not in self._deks:
            self._deks[key_id] = secrets.token_bytes(32)
        return self._deks[key_id]

    def rotate_dek(self, tenant_id: str, collection: str) -> bytes:
        key_id = self._key_id(tenant_id, collection)
        new_key = secrets.token_bytes(32)
        self._deks[key_id] = new_key
        return new_key

    def delete_dek(self, tenant_id: str, collection: str) -> None:
        key_id = self._key_id(tenant_id, collection)
        self._deks.pop(key_id, None)


class EnvelopeEncryption:
    """Encrypt and decrypt data using AES-256-GCM envelope encryption.

    Usage::

        enc = EnvelopeEncryption(key_provider=LocalKeyProvider())
        ciphertext = enc.encrypt(b"sensitive data", tenant_id="acme", collection="patients")
        plaintext  = enc.decrypt(ciphertext, tenant_id="acme", collection="patients")
    """

    def __init__(self, key_provider: KeyProvider) -> None:
        if not _CRYPTO_AVAILABLE:
            raise RuntimeError(
                "cryptography package is required for envelope encryption. "
                "Run: pip install cryptography"
            )
        self._keys = key_provider

    def encrypt(self, data: bytes, *, tenant_id: str, collection: str) -> bytes:
        """Encrypt data and return ciphertext (includes nonce prefix)."""
        dek = self._keys.get_dek(tenant_id, collection)
        nonce = secrets.token_bytes(12)  # 96-bit nonce for AES-GCM
        aesgcm = AESGCM(dek)  # type: ignore[name-defined]
        ct = aesgcm.encrypt(nonce, data, None)
        return nonce + ct

    def decrypt(self, ciphertext: bytes, *, tenant_id: str, collection: str) -> bytes:
        """Decrypt ciphertext — raises if DEK was deleted (GDPR erasure)."""
        dek = self._keys.get_dek(tenant_id, collection)
        nonce, ct = ciphertext[:12], ciphertext[12:]
        aesgcm = AESGCM(dek)  # type: ignore[name-defined]
        return aesgcm.decrypt(nonce, ct, None)


class CryptographicErasure:
    """GDPR Right to Erasure via DEK destruction.

    Instead of deleting individual records (which leaves traces in logs,
    backups, and audit trails), we delete the encryption key.  All data
    encrypted with that key becomes permanently unreadable.

    Audit records are NOT deleted — they record that erasure occurred.
    """

    def __init__(self, key_provider: KeyProvider) -> None:
        self._keys = key_provider

    def erase_collection(
        self, tenant_id: str, collection: str
    ) -> dict[str, str]:
        """Perform cryptographic erasure for a collection.

        Returns:
            Erasure certificate (tenant_id, collection, timestamp, action).
        """
        from datetime import datetime, timezone
        self._keys.delete_dek(tenant_id, collection)
        return {
            "tenant_id": tenant_id,
            "collection": collection,
            "erased_at": datetime.now(timezone.utc).isoformat(),
            "action": "cryptographic_erasure",
            "method": "AES-256-GCM DEK deletion",
        }
