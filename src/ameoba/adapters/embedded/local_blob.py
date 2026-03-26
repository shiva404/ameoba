"""Content-addressed local filesystem blob store.

Stores arbitrary binary payloads on the local filesystem using a
SHA-256 content hash as the file name.  This is the zero-dependency
MVP for blob storage before promoting to S3/MinIO.

Directory layout::

    blobs/
      ab/
        abcdef1234...  ← first 2 chars as subdirectory (avoid inode pressure)
      cd/
        cdef5678...
"""

from __future__ import annotations

import asyncio
import contextlib
import hashlib
import io
from pathlib import Path
from typing import Any, AsyncIterator

import structlog

from ameoba.domain.query import BackendCapabilityManifest, QueryResult, SubPlan
from ameoba.domain.routing import BackendDescriptor, BackendStatus, BackendTier

logger = structlog.get_logger(__name__)


class LocalBlobStore:
    """Content-addressed local blob storage.

    This class satisfies the ``StorageBackend`` protocol for BLOB data.

    Content addressing means:
    - Same content → same path → automatic deduplication
    - No update semantics (immutable blobs)
    - Deletion is cryptographic erasure (remove the key, not the blob)
    """

    BACKEND_ID = "local-blob-embedded"
    SUPPORTED_CATEGORIES = ["blob"]

    def __init__(self, root: Path) -> None:
        self._root = root

    async def open(self) -> None:
        self._root.mkdir(parents=True, exist_ok=True)
        logger.info("local_blob_store_opened", root=str(self._root))

    async def close(self) -> None:
        pass  # Stateless filesystem — nothing to close

    # ------------------------------------------------------------------
    # StorageBackend protocol
    # ------------------------------------------------------------------

    @property
    def descriptor(self) -> BackendDescriptor:
        return BackendDescriptor(
            id=self.BACKEND_ID,
            display_name="Local Filesystem Blob Store",
            tier=BackendTier.EMBEDDED,
            status=BackendStatus.AVAILABLE,
            supported_categories=self.SUPPORTED_CATEGORIES,
            config={"root": str(self._root)},
        )

    @property
    def capabilities(self) -> BackendCapabilityManifest:
        return BackendCapabilityManifest(
            backend_id=self.BACKEND_ID,
            supports_predicate_pushdown=False,
            supports_projection_pushdown=False,
            supports_aggregation_pushdown=False,
            supports_sort_pushdown=False,
            supports_limit_pushdown=False,
            supports_joins=False,
            native_language="none",
        )

    async def health_check(self) -> BackendStatus:
        try:
            self._root.stat()
            return BackendStatus.AVAILABLE
        except OSError:
            return BackendStatus.UNAVAILABLE

    async def write(
        self,
        collection: str,
        records: list[dict[str, Any]],
        *,
        tenant_id: str = "default",
    ) -> list[str]:
        """Write blob payloads.

        Each record should have a ``content`` key with ``bytes`` value.
        Returns the SHA-256 content hash (the blob's stable ID).
        """
        ids: list[str] = []
        for record in records:
            content = record.get("content")
            if not isinstance(content, (bytes, bytearray)):
                # Fallback: encode string content
                if isinstance(content, str):
                    content = content.encode("utf-8")
                else:
                    raise ValueError(f"Blob record must have bytes 'content' field, got {type(content)}")

            blob_id = await self._write_bytes(bytes(content))
            ids.append(blob_id)
        return ids

    async def read(
        self,
        collection: str,
        record_id: str,
        *,
        tenant_id: str = "default",
    ) -> dict[str, Any] | None:
        path = self._blob_path(record_id)
        if not path.exists():
            return None
        content = await asyncio.get_event_loop().run_in_executor(None, path.read_bytes)
        return {"content": content, "content_hash": record_id, "size_bytes": len(content)}

    async def execute_sub_plan(self, sub_plan: SubPlan) -> QueryResult:
        raise NotImplementedError("LocalBlobStore does not support SQL queries")

    async def list_collections(self, *, tenant_id: str = "default") -> list[str]:
        # Blobs are not organised into collections — return a single virtual collection
        return ["blobs"]

    # ------------------------------------------------------------------
    # Blob-specific methods
    # ------------------------------------------------------------------

    async def write_bytes(self, data: bytes) -> str:
        """Write raw bytes and return the content hash."""
        return await self._write_bytes(data)

    async def read_bytes(self, content_hash: str) -> bytes | None:
        path = self._blob_path(content_hash)
        if not path.exists():
            return None
        return await asyncio.get_event_loop().run_in_executor(None, path.read_bytes)

    async def delete(self, content_hash: str) -> bool:
        """Delete a blob by content hash.  Returns True if it existed."""
        path = self._blob_path(content_hash)
        if path.exists():
            path.unlink()
            logger.debug("blob_deleted", content_hash=content_hash)
            return True
        return False

    async def stream_write(self, stream: AsyncIterator[bytes]) -> str:
        """Stream bytes in, compute hash as we go, write to filesystem."""
        hasher = hashlib.sha256()
        buffer = io.BytesIO()

        async for chunk in stream:
            hasher.update(chunk)
            buffer.write(chunk)

        content_hash = hasher.hexdigest()
        data = buffer.getvalue()
        path = self._blob_path(content_hash)

        if not path.exists():
            path.parent.mkdir(parents=True, exist_ok=True)
            await asyncio.get_event_loop().run_in_executor(None, path.write_bytes, data)

        return content_hash

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    async def _write_bytes(self, data: bytes) -> str:
        content_hash = hashlib.sha256(data).hexdigest()
        path = self._blob_path(content_hash)

        if path.exists():
            logger.debug("blob_dedup_hit", content_hash=content_hash)
            return content_hash  # Content-addressed deduplication

        path.parent.mkdir(parents=True, exist_ok=True)
        await asyncio.get_event_loop().run_in_executor(None, path.write_bytes, data)
        logger.debug("blob_written", content_hash=content_hash, size_bytes=len(data))
        return content_hash

    def _blob_path(self, content_hash: str) -> Path:
        """Two-level directory structure to avoid inode pressure."""
        prefix = content_hash[:2]
        return self._root / prefix / content_hash

    async def catalog_stats(
        self,
        *,
        max_files: int = 5000,
        sample_limit: int = 100,
    ) -> dict[str, Any]:
        """Bounded walk of the blob tree: counts, bytes, and sample hashes.

        Stops after ``max_files`` files to keep the catalog endpoint cheap on
        large stores; set ``truncated`` when the cap is hit.
        """
        root = self._root

        def _scan() -> dict[str, Any]:
            n = 0
            total_bytes = 0
            samples: list[str] = []
            truncated = False
            if not root.exists():
                return {
                    "files_scanned": 0,
                    "truncated": False,
                    "total_bytes": 0,
                    "sample_hashes": [],
                }
            for sub in sorted(root.iterdir()):
                if not sub.is_dir() or len(sub.name) != 2:
                    continue
                for f in sorted(sub.iterdir()):
                    if not f.is_file():
                        continue
                    n += 1
                    with contextlib.suppress(OSError):
                        total_bytes += f.stat().st_size
                    if len(samples) < sample_limit:
                        samples.append(f.name)
                    if n >= max_files:
                        truncated = True
                        return {
                            "files_scanned": n,
                            "truncated": truncated,
                            "total_bytes": total_bytes,
                            "sample_hashes": samples,
                        }
            return {
                "files_scanned": n,
                "truncated": truncated,
                "total_bytes": total_bytes,
                "sample_hashes": samples,
            }

        return await asyncio.get_event_loop().run_in_executor(None, _scan)
