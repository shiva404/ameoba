"""Layer 0: Binary / Blob detection.

This layer runs first (priority=10) and is microseconds-fast.  It inspects
magic bytes, Shannon entropy, and null-byte frequency.  If it fires, the
pipeline short-circuits — we skip all JSON/CSV parsing entirely.

Magic byte signatures sourced from the Apache Tika MIME database (subset
covering the most common formats encountered in agentic workflows).
"""

from __future__ import annotations

from typing import Any

from ameoba.domain.record import ClassificationVector
from ameoba.kernel.classifier.heuristics import (
    null_byte_fraction,
    shannon_entropy_bytes,
)

# ---------------------------------------------------------------------------
# Magic byte signatures: (offset, signature_bytes, mime_type_hint)
# ---------------------------------------------------------------------------
_MAGIC_SIGNATURES: list[tuple[int, bytes, str]] = [
    # Images
    (0, b"\xff\xd8\xff", "image/jpeg"),
    (0, b"\x89PNG\r\n\x1a\n", "image/png"),
    (0, b"GIF87a", "image/gif"),
    (0, b"GIF89a", "image/gif"),
    (0, b"RIFF", "image/webp"),       # RIFF....WEBP
    (0, b"\x00\x00\x01\x00", "image/ico"),
    # Documents
    (0, b"%PDF-", "application/pdf"),
    (0, b"\xd0\xcf\x11\xe0", "application/msoffice"),  # OLE2 (doc/xls/ppt)
    (0, b"PK\x03\x04", "application/zip"),              # ZIP (also docx/xlsx)
    # Archives
    (0, b"\x1f\x8b", "application/gzip"),
    (0, b"BZh", "application/bzip2"),
    (0, b"\xfd7zXZ\x00", "application/xz"),
    (0, b"7z\xbc\xaf'\x1c", "application/7z"),
    (0, b"Rar!\x1a\x07", "application/rar"),
    # Columnar data
    (0, b"PAR1", "application/parquet"),  # Apache Parquet
    (0, b"ORC\x0a", "application/orc"),   # Apache ORC
    # Audio/Video
    (0, b"ID3", "audio/mpeg"),
    (0, b"fLaC", "audio/flac"),
    (0, b"\x1aE\xdf\xa3", "video/webm"),  # Matroska / WebM
    # Executables
    (0, b"MZ", "application/x-dosexec"),
    (0, b"\x7fELF", "application/x-elf"),
    (0, b"\xca\xfe\xba\xbe", "application/x-mach-o"),
    # Serialisation formats
    (0, b"\x82\xa7", "application/msgpack"),
    (0, b"OBJAVRO", "application/avro"),
    # SQLite
    (0, b"SQLite format 3\x00", "application/x-sqlite3"),
]

# Entropy and null-byte thresholds come from config; use module-level defaults
# that match the config defaults (avoids importing settings in a hot path).
_DEFAULT_ENTROPY_THRESHOLD = 7.0
_DEFAULT_NULL_BYTE_THRESHOLD = 0.01


class BinaryBlobDetector:
    """Layer 0 classifier — detects binary/blob data before any parsing.

    Priority: 10 (runs first in the cascade).
    """

    priority: int = 10
    name: str = "binary_blob_detector"

    def __init__(
        self,
        entropy_threshold: float = _DEFAULT_ENTROPY_THRESHOLD,
        null_byte_threshold: float = _DEFAULT_NULL_BYTE_THRESHOLD,
    ) -> None:
        self._entropy_threshold = entropy_threshold
        self._null_byte_threshold = null_byte_threshold

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        """Return a high-confidence BLOB result or None to pass to the next layer.

        Checks in order:
        1. Producer content_type claims binary type → high confidence BLOB
        2. Magic byte match → BLOB
        3. High Shannon entropy → BLOB
        4. High null-byte fraction → BLOB
        """
        content_type: str = context.get("content_type") or ""

        # 1. Content-type hint from producer
        if _is_binary_content_type(content_type):
            return ClassificationVector(
                blob=1.0, confidence=0.99, dominant_layer=self.name
            )

        # Only inspect bytes for layers 2–4
        if not isinstance(data, (bytes, bytearray)):
            return None  # Not our concern — pass to next layer

        # 2. Magic bytes
        if _magic_match(data):
            return ClassificationVector(
                blob=1.0, confidence=0.98, dominant_layer=self.name
            )

        # 3. Entropy
        entropy = shannon_entropy_bytes(data)
        if entropy >= self._entropy_threshold:
            return ClassificationVector(
                blob=0.9, confidence=entropy / 8.0, dominant_layer=self.name
            )

        # 4. Null bytes
        null_frac = null_byte_fraction(data)
        if null_frac >= self._null_byte_threshold:
            return ClassificationVector(
                blob=0.85, confidence=min(null_frac * 20, 0.95), dominant_layer=self.name
            )

        return None  # Looks like text bytes — pass to format layer


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _magic_match(data: bytes) -> bool:
    """Return True if any magic signature matches the start of ``data``."""
    for offset, signature, _ in _MAGIC_SIGNATURES:
        end = offset + len(signature)
        if len(data) >= end and data[offset:end] == signature:
            return True
    return False


_BINARY_CONTENT_TYPE_PREFIXES = (
    "image/",
    "video/",
    "audio/",
    "application/octet-stream",
    "application/zip",
    "application/gzip",
    "application/pdf",
    "application/msword",
    "application/vnd.ms-",
    "application/x-",
)


def _is_binary_content_type(content_type: str) -> bool:
    ct = content_type.lower().split(";")[0].strip()
    return any(ct.startswith(prefix) for prefix in _BINARY_CONTENT_TYPE_PREFIXES)
