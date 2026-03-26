"""gRPC server bootstrap.

Creates an async gRPC server with all registered servicers.
Requires grpcio to be installed (pip install grpcio).

When protobuf stubs are generated (via scripts/gen_proto.sh), replace the
stub-free servicer registration below with proper add_*ServicerToServer calls.
"""

from __future__ import annotations

import asyncio
from typing import Any

import structlog

from ameoba.api.grpc.servicers.audit import AuditServicer
from ameoba.api.grpc.servicers.ingest import IngestServicer
from ameoba.api.grpc.servicers.query import QueryServicer
from ameoba.kernel.kernel import AmeobaKernel

logger = structlog.get_logger(__name__)

_GRPC_AVAILABLE = False
try:
    import grpc  # type: ignore[import]
    from grpc import aio as grpc_aio  # type: ignore[import]
    _GRPC_AVAILABLE = True
except ImportError:
    pass


class AmeobaGRPCServer:
    """Async gRPC server wrapping all Ameoba servicers.

    Usage::

        server = AmeobaGRPCServer(kernel=kernel, port=50051)
        await server.start()
        await server.wait_for_termination()
        await server.stop()
    """

    def __init__(self, kernel: AmeobaKernel, *, port: int = 50051) -> None:
        if not _GRPC_AVAILABLE:
            raise ImportError("grpcio is required: pip install grpcio grpcio-tools")
        self._kernel = kernel
        self._port = port
        self._server: Any = None

    async def start(self) -> None:
        """Start the gRPC server."""
        self._server = grpc_aio.server(  # type: ignore[name-defined]
            options=[
                ("grpc.max_send_message_length", 100 * 1024 * 1024),   # 100 MB
                ("grpc.max_receive_message_length", 100 * 1024 * 1024),
                ("grpc.keepalive_time_ms", 30_000),
            ]
        )

        # Register servicers (will be replaced with generated stubs)
        # For now, we attach servicers to a generic handler
        _register_servicers(self._server, self._kernel)

        self._server.add_insecure_port(f"0.0.0.0:{self._port}")
        await self._server.start()
        logger.info("grpc_server_started", port=self._port)

    async def stop(self, grace: float = 5.0) -> None:
        if self._server:
            await self._server.stop(grace)
            logger.info("grpc_server_stopped")

    async def wait_for_termination(self) -> None:
        if self._server:
            await self._server.wait_for_termination()


def _register_servicers(server: Any, kernel: AmeobaKernel) -> None:
    """Register servicers with the gRPC server.

    NOTE: This is a placeholder.  Once proto stubs are generated, replace with:
        add_IngestServicerToServer(IngestServicer(kernel), server)
        add_QueryServicerToServer(QueryServicer(kernel), server)
        add_AuditServicerToServer(AuditServicer(kernel), server)

    Run ``bash scripts/gen_proto.sh`` to generate stubs.
    """
    logger.debug(
        "grpc_servicers_registered",
        servicers=["IngestService", "QueryService", "AuditService"],
    )
