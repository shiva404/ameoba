"""S3-compatible blob storage backend.

Works with AWS S3, MinIO, Cloudflare R2, and any S3-compatible API.
Supports Object Lock (WORM mode) for audit log anchoring.

Dependencies: aiobotocore (pip install aiobotocore) — optional.
"""

from __future__ import annotations

import hashlib
import io
import json
import uuid
from datetime import datetime, timezone
from typing import Any, AsyncIterator

import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier

logger = structlog.get_logger(__name__)

_AIOBOTOCORE_AVAILABLE = False
try:
    import aiobotocore.session  # type: ignore[import]
    _AIOBOTOCORE_AVAILABLE = True
except ImportError:
    pass


class S3BlobStore:
    """S3-compatible content-addressed blob storage.

    Content is addressed by SHA-256 hash — same content is automatically
    deduplicated (content hash == object key).

    Object Lock / WORM support: if ``object_lock=True``, objects are
    written with Compliance mode retention (for audit anchoring).
    """

    SUPPORTED_CATEGORIES = ["blob"]

    def __init__(
        self,
        bucket: str,
        *,
        backend_id: str = "s3-external",
        endpoint_url: str | None = None,
        region: str = "us-east-1",
        access_key: str | None = None,
        secret_key: str | None = None,
        object_lock: bool = False,
        key_prefix: str = "blobs/",
    ) -> None:
        if not _AIOBOTOCORE_AVAILABLE:
            raise ImportError("aiobotocore is required: pip install aiobotocore")

        self._bucket = bucket
        self._backend_id = backend_id
        self._endpoint_url = endpoint_url
        self._region = region
        self._access_key = access_key
        self._secret_key = secret_key
        self._object_lock = object_lock
        self._key_prefix = key_prefix
        self._session: Any = None

    async def open(self) -> None:
        self._session = aiobotocore.session.get_session()  # type: ignore[name-defined]
        logger.info("s3_store_opened", bucket=self._bucket, backend_id=self._backend_id)

    async def close(self) -> None:
        self._session = None

    def _client(self) -> Any:
        """Return a context manager for an async S3 client."""
        kwargs: dict[str, Any] = {
            "region_name": self._region,
        }
        if self._endpoint_url:
            kwargs["endpoint_url"] = self._endpoint_url
        if self._access_key and self._secret_key:
            kwargs["aws_access_key_id"] = self._access_key
            kwargs["aws_secret_access_key"] = self._secret_key
        return self._session.create_client("s3", **kwargs)

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self._backend_id,
            display_name=f"S3 ({self._bucket})",
            tier=BackendTier.EXTERNAL,
            status=BackendStatus.UNKNOWN,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"bucket": self._bucket, "region": self._region},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self._backend_id,
            supports_predicate_pushdown=False,
            supports_projection_pushdown=False,
            supports_aggregation_pushdown=False,
            supports_sort_pushdown=False,
            supports_limit_pushdown=False,
            supports_joins=False,
            native_language="none",
        )

    async def health_check(self) -> BackendStatus:
        if not self._session:
            return BackendStatus.UNAVAILABLE
        try:
            async with self._client() as client:
                await client.head_bucket(Bucket=self._bucket)
            return BackendStatus.AVAILABLE
        except Exception:
            return BackendStatus.UNAVAILABLE

    async def write(
        self,
        collection: str,
        records: list[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> list[str]:
        if not records or not self._session:
            return []

        ids: list[str] = []
        async with self._client() as client:
            for record in records:
                content = record.get("content")
                if isinstance(content, str):
                    content = content.encode("utf-8")
                elif not isinstance(content, (bytes, bytearray)):
                    content = json.dumps(record, default=str).encode("utf-8")

                content_hash = hashlib.sha256(content).hexdigest()
                key = f"{self._key_prefix}{tenant_id}/{content_hash[:2]}/{content_hash}"

                put_kwargs: dict[str, Any] = {
                    "Bucket": self._bucket,
                    "Key": key,
                    "Body": content,
                    "Metadata": {
                        "tenant_id": tenant_id,
                        "collection": collection,
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    },
                }
                await client.put_object(**put_kwargs)
                ids.append(content_hash)

        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        if not self._session:
            return None
        key = f"{self._key_prefix}{tenant_id}/{record_id[:2]}/{record_id}"
        try:
            async with self._client() as client:
                resp = await client.get_object(Bucket=self._bucket, Key=key)
                body = await resp["Body"].read()
            return {"content": body, "content_hash": record_id, "size_bytes": len(body)}
        except Exception:
            return None

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        raise NotImplementedError("S3 does not support SQL queries directly")

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        return ["blobs"]

    async def anchor_digest(self, digest: str, *, label: str = "audit") -> str:
        """Write a tamper-evident audit digest to S3 Object Lock (WORM).

        Returns the S3 key of the anchored object.
        """
        if not self._session:
            raise RuntimeError("S3 store not opened")

        ts = datetime.now(timezone.utc)
        key = f"audit-anchors/{ts.strftime('%Y/%m/%d')}/{label}_{ts.isoformat()}.sha256"
        content = f"{label} {ts.isoformat()} {digest}\n".encode()

        put_kwargs: dict[str, Any] = {
            "Bucket": self._bucket,
            "Key": key,
            "Body": content,
        }
        if self._object_lock:
            from datetime import timedelta
            put_kwargs["ObjectLockMode"] = "COMPLIANCE"
            put_kwargs["ObjectLockRetainUntilDate"] = (ts + timedelta(days=2557))  # 7 years

        async with self._client() as client:
            await client.put_object(**put_kwargs)

        logger.info("audit_digest_anchored", key=key, digest=digest[:16])
        return key
