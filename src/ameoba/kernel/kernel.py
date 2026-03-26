"""The Ameoba microkernel.

This is the central orchestrator that coordinates:
1. Classification (pipeline)
2. Routing (router → topology)
3. Storage (backend adapters)
4. Audit (ledger → sqlite sink)
5. Query (planner → executor)
6. Schema registry (auto-registration on ingest)
7. Staging buffer (queues writes when backends unavailable)

All other code (API, CLI) calls this kernel — never adapters directly.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

import structlog

from ameoba.audit.ledger import AuditLedger
from ameoba.config import Settings
from ameoba.domain.audit import AuditEventKind
from ameoba.domain.query import QueryResult
from ameoba.domain.record import ClassificationVector, DataRecord
from ameoba.domain.routing import BackendStatus, RoutingDecision
from ameoba.kernel.classifier.pipeline import ClassificationPipeline
from ameoba.kernel.router import KernelRouter
from ameoba.kernel.topology import TopologyRegistry
from ameoba.query.executor import QueryExecutor
from ameoba.query.planner import QueryPlanner

logger = structlog.get_logger(__name__)


class IngestResult:
    """Result of a single record ingestion."""

    __slots__ = ("record_id", "classification", "routing", "backend_ids", "audit_sequence")

    def __init__(
        self,
        record_id: uuid.UUID,
        classification: ClassificationVector,
        routing: RoutingDecision,
        backend_ids: list[str],
        audit_sequence: int,
    ) -> None:
        self.record_id = record_id
        self.classification = classification
        self.routing = routing
        self.backend_ids = backend_ids
        self.audit_sequence = audit_sequence

    def __repr__(self) -> str:
        return (
            f"IngestResult(record_id={self.record_id}, "
            f"category={self.classification.primary_category.value}, "
            f"backends={self.backend_ids})"
        )


class AmeobaKernel:
    """The central Ameoba kernel.

    Constructed once at startup and shared across all API handlers and CLI commands.

    Usage::

        kernel = AmeobaKernel(settings)
        await kernel.start()

        result = await kernel.ingest(record)
        query_result = await kernel.query("SELECT * FROM events LIMIT 10")

        await kernel.stop()
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings

        # Subsystems — initialised in start()
        self.topology = TopologyRegistry()
        self.classification_pipeline = ClassificationPipeline(cfg=settings.classifier)
        self.router = KernelRouter(topology=self.topology)
        self.audit_ledger: AuditLedger | None = None
        self.query_planner: QueryPlanner | None = None
        self.query_executor: QueryExecutor | None = None
        self.schema_registry: Any | None = None   # SchemaRegistry (optional)
        self.staging_buffer: Any | None = None    # StagingBuffer

        self._started = False

    async def start(self) -> None:
        """Initialise all subsystems and register embedded backends."""
        if self._started:
            return

        cfg = self._settings.embedded
        cfg.ensure_dirs()

        # 1. Audit ledger (SQLite)
        from ameoba.adapters.embedded.sqlite_audit import SQLiteAuditSink
        audit_sink = SQLiteAuditSink(path=cfg.sqlite_audit_path)
        await audit_sink.open()
        self.audit_ledger = AuditLedger(sink=audit_sink)

        # 2. DuckDB relational store
        from ameoba.adapters.embedded.duckdb_store import DuckDBStore
        duckdb_store = DuckDBStore(path=cfg.duckdb_path)
        await duckdb_store.open()
        await self.topology.register(duckdb_store.descriptor, duckdb_store)

        # 3. Local blob store
        from ameoba.adapters.embedded.local_blob import LocalBlobStore
        blob_store = LocalBlobStore(root=cfg.blob_dir)
        await blob_store.open()
        await self.topology.register(blob_store.descriptor, blob_store)

        # 4. Query engine
        self.query_planner = QueryPlanner(topology=self.topology)
        self.query_executor = QueryExecutor(topology=self.topology)

        # 5. Schema registry (uses same DuckDB store)
        from ameoba.schema.registry import SchemaRegistry
        self.schema_registry = SchemaRegistry(duckdb_store=duckdb_store)
        await self.schema_registry.open()

        # 6. Staging buffer (uses same DuckDB store)
        from ameoba.kernel.staging import StagingBuffer
        self.staging_buffer = StagingBuffer(duckdb_store=duckdb_store)
        await self.staging_buffer.open()

        # 7. Record system start
        await self.audit_ledger.record(
            kind=AuditEventKind.SYSTEM_START,
            detail={"version": "0.1.0", "environment": self._settings.environment},
        )

        self._started = True
        logger.info("ameoba_kernel_started", environment=self._settings.environment)

    async def stop(self) -> None:
        """Gracefully shut down all subsystems."""
        if not self._started:
            return

        if self.audit_ledger:
            await self.audit_ledger.record(kind=AuditEventKind.SYSTEM_STOP)

        # Close backends in reverse registration order
        for desc, backend in reversed(self.topology.list_backends()):
            try:
                await backend.close()
            except Exception:
                logger.exception("backend_close_error", backend_id=desc.id)

        self._started = False
        logger.info("ameoba_kernel_stopped")

    # ------------------------------------------------------------------
    # Core operations
    # ------------------------------------------------------------------

    async def ingest(
        self,
        record: DataRecord,
        *,
        agent_id: str | None = None,
    ) -> IngestResult:
        """Ingest a single DataRecord through the full classification→routing→write pipeline.

        Args:
            record:   The data record to ingest.
            agent_id: Identity of the calling agent (for audit).

        Returns:
            ``IngestResult`` with classification, routing, and audit information.

        Raises:
            RuntimeError: If the kernel has not been started.
        """
        self._assert_started()

        # Stamp ingestion time
        record = record.model_copy(update={"ingested_at": datetime.now(timezone.utc)})

        # --- 1. Classification ---
        vector = self.classification_pipeline.classify(record)
        record = record.model_copy(update={"classification": vector})

        await self.audit_ledger.record(  # type: ignore[union-attr]
            kind=AuditEventKind.CLASSIFICATION,
            agent_id=agent_id or record.agent_id,
            tenant_id=record.tenant_id,
            record_id=record.id,
            collection=record.collection,
            detail={
                "category": vector.primary_category.value,
                "confidence": round(vector.confidence, 4),
                "dominant_layer": vector.dominant_layer,
                "is_mixed": vector.is_mixed,
            },
        )

        # --- 2. Routing ---
        routing = self.router.route(record, vector)

        await self.audit_ledger.record(  # type: ignore[union-attr]
            kind=AuditEventKind.ROUTING,
            agent_id=agent_id or record.agent_id,
            tenant_id=record.tenant_id,
            record_id=record.id,
            collection=record.collection,
            detail={
                "targets": [t.backend_id for t in routing.targets],
                "summary": routing.classification_summary,
            },
        )

        # --- 3. Schema registration (async, non-blocking on error) ---
        if self.schema_registry is not None and isinstance(record.payload, (dict, list)):
            try:
                payload_list = (
                    record.payload if isinstance(record.payload, list) else [record.payload]
                )
                await self.schema_registry.register_from_records(
                    record.collection,
                    payload_list,
                    category=vector.primary_category.value,
                )
            except Exception:
                logger.exception("kernel_schema_registration_error", collection=record.collection)

        # --- 4. Write to backends ---
        backend_ids: list[str] = []
        payload_dict = _record_to_storage_dict(record)

        for target in routing.targets:
            backend = self.topology.get_backend(target.backend_id)
            if backend is None:
                logger.warning(
                    "kernel_backend_not_found",
                    backend_id=target.backend_id,
                    record_id=str(record.id),
                )
                continue

            # Check backend health; stage if unavailable
            backend_status = await backend.health_check()
            if backend_status == BackendStatus.UNAVAILABLE and self.staging_buffer is not None:
                await self.staging_buffer.enqueue(
                    record_id=record.id,
                    backend_id=target.backend_id,
                    collection=target.collection,
                    payload=payload_dict,
                )
                logger.warning(
                    "kernel_backend_unavailable_staged",
                    backend_id=target.backend_id,
                    record_id=str(record.id),
                )
                continue

            try:
                ids = await backend.write(
                    target.collection,
                    [payload_dict],
                    tenant_id=record.tenant_id,
                )
                backend_ids.extend([target.backend_id] * len(ids))

                await self.audit_ledger.record(  # type: ignore[union-attr]
                    kind=AuditEventKind.WRITE,
                    agent_id=agent_id or record.agent_id,
                    tenant_id=record.tenant_id,
                    record_id=record.id,
                    collection=target.collection,
                    backend_id=target.backend_id,
                    detail={"record_ids": ids},
                )
            except Exception as exc:
                logger.exception(
                    "kernel_write_error",
                    backend_id=target.backend_id,
                    record_id=str(record.id),
                    error=str(exc),
                )
                # Stage for retry rather than propagating the error
                if self.staging_buffer is not None:
                    await self.staging_buffer.enqueue(
                        record_id=record.id,
                        backend_id=target.backend_id,
                        collection=target.collection,
                        payload=payload_dict,
                    )
                else:
                    raise

        audit_seq = self.audit_ledger.sequence  # type: ignore[union-attr]

        return IngestResult(
            record_id=record.id,
            classification=vector,
            routing=routing,
            backend_ids=backend_ids,
            audit_sequence=audit_seq,
        )

    async def ingest_batch(
        self,
        records: list[DataRecord],
        *,
        agent_id: str | None = None,
    ) -> list[IngestResult]:
        """Ingest multiple records sequentially.

        For production throughput, consider using the gRPC streaming API instead.
        """
        results = []
        for record in records:
            result = await self.ingest(record, agent_id=agent_id)
            results.append(result)
        return results

    async def query(
        self,
        sql: str,
        *,
        tenant_id: str = "default",
        agent_id: str | None = None,
    ) -> QueryResult:
        """Execute a SQL query against the registered backends.

        Args:
            sql:       Federated SQL (see ARCHITECTURE.md §5).
            tenant_id: Tenant isolation filter.
            agent_id:  For audit.

        Returns:
            ``QueryResult`` with columns, rows, and execution metadata.
        """
        self._assert_started()

        plan = self.query_planner.plan(sql, tenant_id=tenant_id)  # type: ignore[union-attr]
        result = await self.query_executor.execute(plan)  # type: ignore[union-attr]

        await self.audit_ledger.record(  # type: ignore[union-attr]
            kind=AuditEventKind.QUERY,
            agent_id=agent_id,
            tenant_id=tenant_id,
            detail={
                "sql": sql[:500],
                "path": plan.path.value,
                "row_count": result.row_count,
                "backends": result.backend_ids_used,
                "elapsed_ms": round(result.execution_ms, 2),
            },
        )

        return result

    async def audit_verify(self) -> tuple[bool, str]:
        """Verify the integrity of the audit ledger."""
        self._assert_started()
        return await self.audit_ledger.verify_integrity()  # type: ignore[union-attr]

    async def health(self) -> dict[str, Any]:
        """Return health status for all registered backends."""
        statuses = await self.topology.health_check_all()
        staged = 0
        if self.staging_buffer is not None:
            try:
                staged = await self.staging_buffer.pending_count()
            except Exception:
                pass
        return {
            "kernel": "ok",
            "backends": {k: v.value for k, v in statuses.items()},
            "audit_sequence": self.audit_ledger.sequence if self.audit_ledger else 0,
            "staging_pending": staged,
        }

    async def flush_staging(self) -> dict[str, int]:
        """Attempt to flush all staged records back to their target backends.

        Returns a mapping of backend_id → flushed_count.
        """
        self._assert_started()
        if self.staging_buffer is None:
            return {}

        results: dict[str, int] = {}
        for desc, backend in self.topology.list_backends():
            count = await self.staging_buffer.flush(desc.id, backend)
            if count:
                results[desc.id] = count
        return results

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _assert_started(self) -> None:
        if not self._started:
            raise RuntimeError("AmeobaKernel has not been started — call await kernel.start() first")


def _record_to_storage_dict(record: DataRecord) -> dict[str, Any]:
    """Convert a DataRecord to a flat dict suitable for storage backends."""
    payload = record.payload

    if isinstance(payload, dict):
        base = dict(payload)
    elif isinstance(payload, (str, bytes, bytearray)):
        base = {"content": payload}
    else:
        import json
        try:
            base = {"content": json.dumps(payload, default=str)}
        except (TypeError, ValueError):
            base = {"content": str(payload)}

    base["_record_id"] = str(record.id)
    base["_collection"] = record.collection
    base["_lifecycle"] = record.lifecycle.value
    base["_created_at"] = record.created_at.isoformat()
    if record.ingested_at:
        base["_ingested_at"] = record.ingested_at.isoformat()

    return base
