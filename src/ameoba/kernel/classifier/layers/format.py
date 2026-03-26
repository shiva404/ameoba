"""Layer 1: Format detection.

Determines whether data is JSON, CSV, XML, Parquet, Avro, or plain text.
Sub-millisecond for typical payloads.

If data is bytes, attempts UTF-8 decode before structural inspection.
If it is already a parsed Python object (dict / list), skips this layer.
"""

from __future__ import annotations

import csv
import io
import json
from typing import Any

from ameoba.domain.record import ClassificationVector


class FormatDetector:
    """Layer 1 classifier — identifies the serialisation format of the data.

    Priority: 20.
    """

    priority: int = 20
    name: str = "format_detector"

    def classify(self, data: Any, context: dict[str, Any]) -> ClassificationVector | None:
        """Attempt to identify the data format and decode it.

        Side effect: if successful, writes the decoded Python object back into
        ``context["decoded"]`` so subsequent layers don't re-parse.

        Returns:
            A ``ClassificationVector`` only when data is structurally unambiguous
            (e.g. confirmed XML → document).  Returns None for ambiguous formats
            so structural and semantic layers can weigh in.
        """
        # If already a Python object, nothing to detect
        if isinstance(data, (dict, list)):
            context.setdefault("decoded", data)
            return None  # Let structural layer decide

        if not isinstance(data, (bytes, bytearray, str)):
            return None

        text = _to_text(data)
        if text is None:
            return None  # Binary — should have been caught by Layer 0

        text = text.strip()
        if not text:
            return None

        # JSON
        parsed = _try_json(text, context)
        if parsed is not None:
            context["decoded"] = parsed
            context["format"] = "json"
            return None  # Let structural/semantic layers classify the content

        # XML / HTML → document
        if text.startswith("<") and ">" in text:
            context["format"] = "xml"
            context["decoded"] = text
            return ClassificationVector(
                document=0.85, confidence=0.85, dominant_layer=self.name
            )

        # CSV heuristic
        if _looks_like_csv(text):
            context["format"] = "csv"
            rows = _parse_csv(text)
            if rows:
                context["decoded"] = rows
            return ClassificationVector(
                relational=0.85, confidence=0.8, dominant_layer=self.name
            )

        # Parquet magic (PAR1) should have been caught by Layer 0 already
        # but handle gracefully
        if isinstance(data, (bytes, bytearray)) and data[:4] == b"PAR1":
            context["format"] = "parquet"
            return ClassificationVector(
                relational=0.9, confidence=0.95, dominant_layer=self.name
            )

        # Avro magic: 4-byte marker + schema JSON embedded
        if isinstance(data, (bytes, bytearray)) and data[:4] == b"Obj\x01":
            context["format"] = "avro"
            return ClassificationVector(
                relational=0.8, confidence=0.9, dominant_layer=self.name
            )

        # Plain text → document by default
        context["format"] = "text"
        context["decoded"] = text
        return ClassificationVector(
            document=0.6, confidence=0.5, dominant_layer=self.name
        )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _to_text(data: bytes | bytearray | str) -> str | None:
    if isinstance(data, str):
        return data
    try:
        return data.decode("utf-8")
    except (UnicodeDecodeError, AttributeError):
        return None  # Not valid UTF-8 text


def _try_json(text: str, context: dict[str, Any]) -> Any:
    """Attempt JSON parse. Returns parsed object or None."""
    try:
        return json.loads(text)
    except (json.JSONDecodeError, ValueError):
        return None


def _looks_like_csv(text: str) -> bool:
    """Heuristic: does the text look like CSV?"""
    # Take first 2 KB for speed
    sample = text[:2048]
    lines = sample.splitlines()
    if len(lines) < 2:
        return False

    # Check for common delimiters and consistent column counts
    for delimiter in (",", "\t", ";", "|"):
        counts = [line.count(delimiter) for line in lines[:10] if line.strip()]
        if counts and min(counts) > 0 and max(counts) == min(counts):
            return True

    return False


def _parse_csv(text: str) -> list[dict[str, Any]] | None:
    """Parse CSV text into a list of dicts (first row = headers)."""
    try:
        reader = csv.DictReader(io.StringIO(text))
        rows = [dict(row) for row in reader]
        return rows if rows else None
    except Exception:
        return None
