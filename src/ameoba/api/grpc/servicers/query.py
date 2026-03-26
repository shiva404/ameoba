"""gRPC QueryServicer — streams query result rows."""

from __future__ import annotations

import json
from typing import Any, AsyncIterator

import structlog

from ameoba.kernel.kernel import AmeobaKernel

logger = structlog.get_logger(__name__)


class QueryServicer:
    """gRPC servicer for the QueryService."""

    def __init__(self, kernel: AmeobaKernel) -> None:
        self._kernel = kernel

    async def Execute(
        self,
        request: Any,
        context: Any,
    ) -> AsyncIterator[dict[str, Any]]:
        """Stream query results: first message is schema, then data rows."""
        sql = getattr(request, "sql", "")
        tenant_id = getattr(request, "tenant_id", "default") or "default"
        max_rows = int(getattr(request, "max_rows", 1000) or 1000)

        try:
            result = await self._kernel.query(sql, tenant_id=tenant_id)
        except Exception as exc:
            logger.exception("grpc_query_error")
            yield {"columns": [], "values": [], "is_schema": False, "error": str(exc)}
            return

        # First: schema message
        yield {
            "columns": result.columns,
            "values": [],
            "is_schema": True,
            "row_count": result.row_count,
            "execution_ms": result.execution_ms,
        }

        # Then: data rows (one per gRPC message)
        for row in result.rows[:max_rows]:
            yield {
                "columns": [],
                "values": [json.dumps(v, default=str) for v in row],
                "is_schema": False,
                "row_count": 0,
                "execution_ms": 0.0,
            }
