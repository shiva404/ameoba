"""gRPC IngestServicer — handles IngestOne and IngestStream RPCs.

Stub-compatible: works whether protobuf stubs are generated or not.
To generate stubs:
    cd proto && python -m grpc_tools.protoc -I. --python_out=../src --grpc_python_out=../src ameoba/v1/ingest.proto

Until stubs are generated, this servicer is wired directly in server.py
using the reflection-free approach.
"""

from __future__ import annotations

import json
import uuid
from typing import Any, AsyncIterator

import structlog

from ameoba.domain.record import DataCategory, DataLifecycle, DataRecord
from ameoba.kernel.kernel import AmeobaKernel

logger = structlog.get_logger(__name__)


class IngestServicer:
    """gRPC servicer for the IngestService.

    Depends on an ``AmeobaKernel`` instance injected at construction.
    """

    def __init__(self, kernel: AmeobaKernel) -> None:
        self._kernel = kernel

    async def IngestOne(self, request: Any, context: Any) -> Any:
        """Handle a single unary ingest RPC."""
        try:
            record = _request_to_record(request)
            result = await self._kernel.ingest(record)
            return _build_response(result)
        except Exception as exc:
            logger.exception("grpc_ingest_one_error")
            return _error_response(str(exc))

    async def IngestStream(
        self,
        request_iterator: AsyncIterator[Any],
        context: Any,
    ) -> AsyncIterator[Any]:
        """Handle a bidirectional streaming ingest RPC."""
        async for request in request_iterator:
            try:
                record = _request_to_record(request)
                result = await self._kernel.ingest(record)
                yield _build_response(result)
            except Exception as exc:
                logger.exception("grpc_ingest_stream_error")
                yield _error_response(str(exc))


def _request_to_record(request: Any) -> DataRecord:
    payload_raw = getattr(request, "payload_json", b"{}")
    if isinstance(payload_raw, bytes):
        payload_raw = payload_raw.decode("utf-8")
    try:
        payload = json.loads(payload_raw)
    except json.JSONDecodeError:
        payload = payload_raw

    category_hint = None
    hint_str = getattr(request, "category_hint", "") or ""
    if hint_str:
        try:
            category_hint = DataCategory(hint_str.lower())
        except ValueError:
            pass

    lifecycle_str = getattr(request, "lifecycle", "raw") or "raw"
    try:
        lifecycle = DataLifecycle(lifecycle_str.lower())
    except ValueError:
        lifecycle = DataLifecycle.RAW

    return DataRecord(
        id=uuid.uuid4(),
        collection=getattr(request, "collection", "default"),
        payload=payload,
        content_type=getattr(request, "content_type", None) or None,
        category_hint=category_hint,
        lifecycle=lifecycle,
        tenant_id=getattr(request, "tenant_id", "default") or "default",
        agent_id=getattr(request, "agent_id", None) or None,
        session_id=getattr(request, "session_id", None) or None,
    )


def _build_response(result: Any) -> Any:
    """Build a gRPC response object — returns a plain dict for stub-free use."""
    return {
        "record_id": str(result.record_id),
        "category": result.classification.primary_category.value,
        "confidence": result.classification.confidence,
        "backend_ids": result.backend_ids,
        "audit_sequence": result.audit_sequence,
        "error": "",
    }


def _error_response(error: str) -> Any:
    return {
        "record_id": "",
        "category": "",
        "confidence": 0.0,
        "backend_ids": [],
        "audit_sequence": 0,
        "error": error,
    }
