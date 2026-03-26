# Ameoba

**Intelligent adaptive data fabric for agentic workflows.**

Ameoba is an orchestration and routing layer that sits between AI agents and their storage backends. It accepts heterogeneous data from agents, automatically classifies it by shape and content, routes it to the right backend, and maintains a tamper-evident audit trail of every operation.

It is not a database — it is the layer that decides *which* database a record belongs in, writes it there, tracks what happened, and lets you query across all of them with a single SQL interface.

```
Agent  →  POST /v1/ingest  →  [Classify]  →  [Route]  →  DuckDB / Postgres / Neo4j / ES / S3 / LanceDB
                                                            ↓
                                                       [Audit Ledger (SQLite)]
                                                            ↓
                                               GET /v1/query  →  Federated SQL result
```

---

## Table of Contents

1. [Key Features](#key-features)
2. [Architecture Overview](#architecture-overview)
3. [Quick Start](#quick-start)
4. [Developer Setup](#developer-setup)
5. [Configuration](#configuration)
6. [HTTP API](#http-api)
7. [CLI Reference](#cli-reference)
8. [Python SDK](#python-sdk)
9. [Backend Integrations](#backend-integrations)
10. [Security & Auth](#security--auth)
11. [Schema Registry](#schema-registry)
12. [Federated Query](#federated-query)
13. [Audit & Compliance](#audit--compliance)
14. [gRPC API](#grpc-api)
15. [Observability](#observability)
16. [Testing](#testing)
17. [Deployment](#deployment)

---

## Key Features

| Feature | Detail |
|---|---|
| **Auto-classification** | 4-layer cascade (binary → format → structural → semantic). Rule-based — deterministic and auditable. |
| **Polyglot routing** | Routes records to DuckDB, Postgres, Neo4j, Elasticsearch, S3, or LanceDB based on data shape. |
| **Federated SQL** | Single SQL interface across all backends. Cross-backend joins computed in DuckDB. |
| **Immutable audit ledger** | RFC 6962 Merkle tree + hash-chained SQLite. O(log n) inclusion proofs. |
| **Schema registry** | Auto-infers JSON Schema on every ingest. Tracks versions, compatibility, drift. |
| **Staging buffer** | Queues writes when a backend is unavailable. Exponential-backoff retry. |
| **Envelope encryption** | KMS master → tenant KEK → collection DEK. GDPR erasure via DEK destruction. |
| **Cedar-compatible authz** | Scope + label-based access control. PHI/PCI/PII require explicit grants. |
| **CRDTs** | G-Set / OR-Set / LWW-Register / HLC for partition-tolerant audit capture and schema evolution. |
| **gRPC + HTTP** | Full REST API and gRPC streaming (IngestStream, QueryExecute, AuditTail). |
| **Zero-dependency start** | Runs fully embedded (DuckDB + SQLite + local blobs). External backends are optional. |

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          API Layer                                       │
│   FastAPI HTTP  ·  gRPC (IngestService / QueryService / AuditService)   │
└───────────────────────────┬─────────────────────────────────────────────┘
                             │
┌───────────────────────────▼─────────────────────────────────────────────┐
│                       AmeobaKernel                                       │
│                                                                          │
│  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  ┌─────────────┐ │
│  │Classification│  │    Router    │  │Schema Registry│  │Staging Buf  │ │
│  │  Pipeline    │  │ (topology-   │  │(auto-register │  │(retry queue)│ │
│  │  (4 layers)  │  │  aware)      │  │ on ingest)    │  │             │ │
│  └──────┬───────┘  └──────┬───────┘  └──────────────┘  └─────────────┘ │
│         │                 │                                              │
│  ┌──────▼─────────────────▼───────────────────────────────────────────┐ │
│  │              TopologyRegistry  (all registered backends)            │ │
│  └──────┬──────────┬────────┬─────────┬───────────┬───────────┬──────┘ │
└─────────┼──────────┼────────┼─────────┼───────────┼───────────┼────────┘
          │          │        │         │           │           │
     ┌────▼───┐ ┌────▼──┐ ┌──▼──┐ ┌────▼────┐ ┌───▼──┐  ┌────▼──┐
     │DuckDB  │ │SQLite │ │Blob │ │Postgres │ │Neo4j │  │  ES   │
     │(relat) │ │(audit)│ │(fs) │ │         │ │(graph│  │(docs) │
     └────────┘ └───────┘ └─────┘ └─────────┘ └──────┘  └───────┘
                                                          ┌──────┐ ┌──────┐
                                                          │  S3  │ │Lance │
                                                          │(blob)│ │DB    │
                                                          └──────┘ └──────┘
```

### Classification Pipeline

```
Payload  →  [L0: Binary/Blob]  →  magic bytes, entropy, null-byte %
                 ↓ (if not blob)
            [L1: Format]       →  JSON? CSV? XML? Parquet?
                 ↓
            [L2: Structural]   →  flatness, key consistency, nesting depth
                 ↓
            [L3: Semantic]     →  graph topology, domain vocabulary, embeddings
                 ↓
        ClassificationVector {relational: 0.7, document: 0.2, graph: 0.0, ...}
```

Output is a probability distribution, not a single label. Mixed data is routed to multiple backends simultaneously.

---

## Quick Start

### Embedded (no external services needed)

```bash
# Install core
pip install ameoba

# Ingest a record
echo '{"user_id": "u1", "event": "login", "ts": "2024-01-01T00:00:00Z"}' \
  | ameoba ingest - --collection events

# Query it back
ameoba query "SELECT * FROM events LIMIT 10"

# Verify audit integrity
ameoba audit verify
```

### HTTP Server

```bash
# Start the server (defaults: port 8000, embedded DuckDB + SQLite)
ameoba serve

# Ingest via HTTP
curl -X POST http://localhost:8000/v1/ingest \
  -H "Content-Type: application/json" \
  -d '{"collection": "events", "payload": {"user_id": "u1", "event": "login"}}'

# Query
curl -X POST http://localhost:8000/v1/query \
  -H "Content-Type: application/json" \
  -d '{"sql": "SELECT * FROM events LIMIT 10"}'

# Docs
open http://localhost:8000/docs
```

---

## Developer Setup

### Prerequisites

- Python 3.11+
- [uv](https://github.com/astral-sh/uv) (recommended) or pip

### Install

```bash
git clone <repo>
cd ameoba

# Create venv and install all dependencies including dev extras
uv venv
source .venv/bin/activate
uv pip install -e ".[dev]"

# Run the test suite
pytest

# Run with specific extras (e.g. Postgres + LanceDB)
uv pip install -e ".[postgres,lancedb,dev]"
```

### Optional extras

| Extra | Installs | Use when |
|---|---|---|
| `postgres` | asyncpg, SQLAlchemy, Alembic | Writing to PostgreSQL |
| `neo4j` | neo4j driver | Writing to Neo4j |
| `elasticsearch` | elasticsearch[async] | Writing to Elasticsearch / OpenSearch |
| `s3` | aiobotocore | Writing to S3 / MinIO / R2 |
| `lancedb` | lancedb, pyarrow | Vector storage |
| `grpc` | grpcio, grpcio-tools | gRPC server and client |
| `auth` | python-jose, cryptography | JWT validation, envelope encryption |
| `all` | Everything above | Full stack |

```bash
uv pip install -e ".[all,dev]"
```

### Generate gRPC stubs

Proto definitions live in `proto/ameoba/v1/`. After installing the `grpc` extra:

```bash
bash scripts/gen_proto.sh
# Outputs to: src/ameoba/api/grpc/pb/
```

---

## Configuration

All settings are read from environment variables or a `.env` file in the working directory. Sub-configs are prefixed:

| Prefix | Config class | Key settings |
|---|---|---|
| `AMEOBA_EMBEDDED_` | `EmbeddedConfig` | `DATA_DIR` (default `~/.ameoba/data`) |
| `AMEOBA_API_` | `APIConfig` | `HOST`, `PORT` (8000), `GRPC_PORT` (50051) |
| `AMEOBA_AUTH_` | `AuthConfig` | `API_KEY_ENABLED`, `API_KEYS`, `JWT_SECRET`, `JWT_ALGORITHM` |
| `AMEOBA_OBS_` | `ObservabilityConfig` | `LOG_LEVEL`, `LOG_FORMAT`, `OTLP_ENDPOINT`, `SERVICE_NAME` |
| `AMEOBA_CLASSIFIER_` | `ClassifierConfig` | Thresholds for all classification layers |
| *(root)* | `Settings` | `ENVIRONMENT` (`development` / `staging` / `production`) |

### Minimal `.env`

```env
AMEOBA_EMBEDDED_DATA_DIR=/var/lib/ameoba
AMEOBA_API_PORT=8000
AMEOBA_AUTH_JWT_SECRET=change-me-in-production
AMEOBA_ENVIRONMENT=production
AMEOBA_OBS_LOG_FORMAT=json
AMEOBA_OBS_OTLP_ENDPOINT=http://otel-collector:4317
```

### Classifier tuning

```env
# Relational classification: minimum Jaccard key similarity
AMEOBA_CLASSIFIER_RELATIONAL_JACCARD_THRESHOLD=0.85

# Binary blob: Shannon entropy threshold (bits/byte)
AMEOBA_CLASSIFIER_BLOB_ENTROPY_THRESHOLD=7.0

# Records larger than this go directly to blob store
AMEOBA_CLASSIFIER_DIRECT_BLOB_SIZE_BYTES=1073741824

# Schema drift: re-check schema every N records
AMEOBA_CLASSIFIER_SCHEMA_DRIFT_WINDOW=100
```

---

## HTTP API

Base URL: `http://localhost:8000`

Interactive docs: `GET /docs` (Swagger) · `GET /redoc`

### Ingest

```http
POST /v1/ingest
Content-Type: application/json

{
  "collection": "users",
  "payload": {"id": "u1", "name": "Alice", "email": "alice@example.com"},
  "tenant_id": "acme",
  "lifecycle": "final",
  "category_hint": "relational"
}
```

Response:

```json
{
  "record_id": "550e8400-e29b-41d4-a716-446655440000",
  "category": "relational",
  "confidence": 0.91,
  "backend_ids": ["duckdb-embedded"],
  "audit_sequence": 42,
  "is_mixed": false
}
```

**Batch ingest** (up to 1000 records, HTTP 207 multi-status):

```http
POST /v1/ingest/batch
Content-Type: application/json

{"records": [{...}, {...}]}
```

### Query

```http
POST /v1/query
Content-Type: application/json

{
  "sql": "SELECT * FROM users WHERE name LIKE 'Ali%' LIMIT 10",
  "tenant_id": "acme",
  "max_rows": 1000
}
```

Response:

```json
{
  "columns": ["id", "name", "email"],
  "rows": [["u1", "Alice", "alice@example.com"]],
  "row_count": 1,
  "execution_ms": 3.2,
  "backend_ids_used": ["duckdb-embedded"]
}
```

### Schema registry

```http
# List collections with registered schemas
GET /v1/schema

# Latest schema for a collection
GET /v1/schema/{collection}

# All versions (newest first)
GET /v1/schema/{collection}/versions

# Infer and register a schema from sample records
POST /v1/schema/{collection}/infer
{"records": [{...}, {...}]}
```

### Audit

```http
# Paginated event tail
GET /v1/audit/tail?limit=50&offset=0

# Verify full ledger integrity (Merkle tree + hash chain)
GET /v1/audit/verify
```

### Health

```http
GET /v1/health     # Always 200 {"status": "ok"}
GET /v1/ready      # 200 when kernel started, 503 otherwise
```

---

## CLI Reference

```
ameoba --help

Commands:
  ingest    Ingest data from a file or stdin
  query     Run a SQL query
  audit     Audit trail commands
  backend   Storage backend management
  serve     Start the HTTP (and optionally gRPC) server
  version   Print version
```

### ingest

```bash
# From a JSON file (single record or JSON array)
ameoba ingest records.json --collection events

# From stdin
echo '{"event": "click"}' | ameoba ingest - --collection events

# With explicit options
ameoba ingest data.json \
  --collection orders \
  --tenant-id acme \
  --lifecycle final \
  --category relational
```

### query

```bash
# Basic query (table output by default)
ameoba query "SELECT * FROM events LIMIT 10"

# JSON output
ameoba query "SELECT count(*) FROM events" --format json

# CSV output
ameoba query "SELECT * FROM users" --format csv > users.csv

# Specify tenant
ameoba query "SELECT * FROM orders" --tenant-id acme
```

### audit

```bash
# Verify ledger integrity (prints OK or FAIL + reason)
ameoba audit verify

# Show recent events
ameoba audit tail --limit 20

# Show events for a specific record
ameoba audit tail --record-id <uuid>
```

### backend

```bash
# List registered backends and their status
ameoba backend list

# Run health checks
ameoba backend health

# Flush staged (queued) records back to their backends
ameoba backend flush-staging

# Show count of pending staged records
ameoba backend pending
ameoba backend pending --backend postgres-primary
```

### serve

```bash
# Start with defaults (port 8000, development mode)
ameoba serve

# Production
ameoba serve --port 8000 --workers 4

# With gRPC
ameoba serve --grpc --grpc-port 50051
```

---

## Python SDK

Use `AmeobaKernel` directly in your code — no HTTP required.

```python
import asyncio
from ameoba.kernel.kernel import AmeobaKernel
from ameoba.domain.record import DataRecord
from ameoba.config import Settings

async def main():
    kernel = AmeobaKernel(Settings())
    await kernel.start()

    # Ingest a record
    record = DataRecord(
        collection="orders",
        payload={"order_id": "001", "amount": 99.99, "status": "pending"},
        tenant_id="acme",
    )
    result = await kernel.ingest(record)
    print(f"Classified as: {result.classification.primary_category.value}")
    print(f"Written to:    {result.backend_ids}")

    # Query
    qr = await kernel.query("SELECT * FROM orders LIMIT 10", tenant_id="acme")
    for row in qr.rows:
        print(dict(zip(qr.columns, row)))

    # Verify audit
    ok, msg = await kernel.audit_verify()
    print(f"Audit: {msg}")

    await kernel.stop()

asyncio.run(main())
```

### Batch ingest

```python
results = await kernel.ingest_batch(records, agent_id="my-agent")
```

### Schema registry

```python
# Get latest inferred schema for a collection
schema_version = await kernel.schema_registry.get_latest("orders")
print(schema_version.json_schema)
print(f"Version: {schema_version.version_number}")
print(f"Compatibility: {schema_version.compatibility.value}")

# Register from sample records manually
version = await kernel.schema_registry.register_from_records(
    "orders", sample_records, category="relational"
)
```

### Staging buffer

```python
# Check how many records are waiting to be retried
count = await kernel.staging_buffer.pending_count()

# Flush to all backends
flushed = await kernel.flush_staging()
# → {"postgres-primary": 12, "neo4j-external": 3}
```

### Registering external backends

```python
from ameoba.adapters.postgres.store import PostgresStore

pg = PostgresStore(dsn="postgresql+asyncpg://user:pass@localhost/db")
await pg.open()
await kernel.topology.register(pg.descriptor, pg)
```

---

## Backend Integrations

All external backends are **optional**. Ameoba starts with zero infrastructure (DuckDB + SQLite + local filesystem).

### DuckDB (embedded, always active)

No configuration needed. Handles **relational** data. Acts as the federation compute engine for cross-backend joins.

```
AMEOBA_EMBEDDED_DATA_DIR=~/.ameoba/data   # DuckDB file: $DATA_DIR/ameoba.duckdb
```

### SQLite Audit Ledger (embedded, always active)

```
# File: $DATA_DIR/audit.sqlite
# Append-only enforced via BEFORE UPDATE/DELETE triggers
```

### Local Blob Store (embedded, always active)

SHA-256 content-addressed storage. Automatic deduplication.

```
# Directory: $DATA_DIR/blobs/
# Two-level: blobs/ab/abcdef1234...
```

### PostgreSQL

```bash
pip install "ameoba[postgres]"
```

```python
from ameoba.adapters.postgres.store import PostgresStore

pg = PostgresStore(
    dsn="postgresql+asyncpg://ameoba:secret@localhost:5432/mydb",
    backend_id="postgres-primary",
    schema="ameoba",
)
await pg.open()
await kernel.topology.register(pg.descriptor, pg)
```

**What it does:** Creates tables on first write (schema inferred from records), uses `ON CONFLICT DO NOTHING` for idempotent ingest. Handles **relational** data category.

### Neo4j

```bash
pip install "ameoba[neo4j]"
```

```python
from ameoba.adapters.neo4j.store import Neo4jStore

neo4j = Neo4jStore(
    uri="bolt://localhost:7687",
    user="neo4j",
    password="secret",
    backend_id="neo4j-kg",
    database="neo4j",
)
await neo4j.open()
await kernel.topology.register(neo4j.descriptor, neo4j)
```

**What it does:** Writes graph records as labelled property graph nodes and edges. If payload has `nodes` and `edges` keys, both are written; otherwise the whole record is a single node. Uses `MERGE` to avoid duplicates. Handles **graph** data category.

**Graph payload format:**

```python
record = DataRecord(
    collection="knowledge_graph",
    payload={
        "nodes": [
            {"id": "n1", "label": "Person", "name": "Alice"},
            {"id": "n2", "label": "Company", "name": "Acme Inc"},
        ],
        "edges": [
            {"source": "n1", "target": "n2", "type": "WORKS_AT"},
        ]
    }
)
```

### Elasticsearch / OpenSearch

```bash
pip install "ameoba[elasticsearch]"
```

```python
from ameoba.adapters.elasticsearch.store import ElasticsearchStore

es = ElasticsearchStore(
    hosts=["http://localhost:9200"],
    backend_id="elasticsearch-primary",
    index_prefix="ameoba_",
    refresh_on_write="false",  # use "true" in tests for immediate visibility
)
await es.open()
await kernel.topology.register(es.descriptor, es)
```

**What it does:** Each collection becomes an ES index (`ameoba_{collection}`). Supports full-text search via the `full_text_search` TVF. Also handles kNN vector search for co-located embeddings. Handles **document** data category.

### S3 / MinIO / Cloudflare R2

```bash
pip install "ameoba[s3]"
```

```python
from ameoba.adapters.s3.store import S3BlobStore

s3 = S3BlobStore(
    bucket="my-ameoba-bucket",
    backend_id="s3-primary",
    endpoint_url="http://minio:9000",  # omit for AWS S3
    access_key="minioadmin",
    secret_key="minioadmin",
    region="us-east-1",
    object_lock=True,   # enable WORM for audit anchoring
)
await s3.open()
await kernel.topology.register(s3.descriptor, s3)
```

**What it does:** Content-addressed blob storage (SHA-256 keyed). Automatic deduplication. With `object_lock=True`, uses S3 Object Lock in Compliance mode (7-year retention) for tamper-evident audit anchoring. Handles **blob** data category.

### LanceDB (vector storage)

```bash
pip install "ameoba[lancedb]"
```

```python
from ameoba.adapters.lancedb.store import LanceDBStore

lance = LanceDBStore(
    uri="./vectors",          # or "s3://my-bucket/vectors" for S3-backed
    backend_id="lancedb-primary",
    vector_field="embedding",
    metric="cosine",
)
await lance.open()
await kernel.topology.register(lance.descriptor, lance)
```

**What it does:** Approximate nearest-neighbour search via IVF-PQ or HNSW. Apache Arrow-native, zero-copy with DuckDB. Auto-detects the vector field (fixed-size float list). Handles **vector** data category.

**Ingest with embeddings:**

```python
record = DataRecord(
    collection="doc_embeddings",
    payload={
        "id": "doc-1",
        "text": "Introduction to machine learning",
        "embedding": [0.1, 0.2, 0.3, ...]  # 384/768/1536-dim float list
    },
)
```

---

## Security & Auth

### API Keys (development / testing)

```env
AMEOBA_AUTH_API_KEY_ENABLED=true
AMEOBA_AUTH_API_KEYS=amk_abc123,amk_def456
```

```bash
curl -H "X-API-Key: amk_abc123" http://localhost:8000/v1/query ...
```

Generate a key programmatically:

```python
from ameoba.security.authn.api_key import APIKeyStore
print(APIKeyStore.generate_key("amk"))  # → amk_Xt2...
```

### JWT / OAuth2 (production)

```env
AMEOBA_AUTH_JWT_SECRET=your-hs256-secret-or-path-to-rsa-public-key
AMEOBA_AUTH_JWT_ALGORITHM=HS256  # or RS256
```

```bash
curl -H "Authorization: Bearer <jwt>" http://localhost:8000/v1/query ...
```

JWT claims mapped to `AgentIdentity`:

| JWT claim | AgentIdentity field |
|---|---|
| `sub` | `agent_id` |
| `tid` or `tenant_id` | `tenant_id` |
| `scope` (space-separated) | `scopes` |
| `act.sub` (RFC 8693) | `delegated_by` |

### Authorization (Cedar-compatible)

Access control is enforced per-request via `SimplePolicyEngine` (pure-Python Cedar fallback):

- **Default deny** — every request requires an explicit scope
- **Scope check** — `read` for queries, `write` for ingest, `admin` for all
- **Label-based access** — `PHI`, `PCI`, `PII` labels require explicit grants
- **Delegation depth** — max 3 levels (RFC 8693)
- **Tenant isolation** — `_tenant_id` row filter injected into all queries

```python
from ameoba.security.authz.cedar_engine import SimplePolicyEngine

engine = SimplePolicyEngine(
    restricted_label_grants={
        "PHI": {"agent-hipaa-authorized", "agent-clinician"},
    }
)
```

### Agent-to-Agent Delegation (RFC 8693)

```python
from ameoba.security.authz.delegation import create_delegation

# Agent A delegates a read-only sub-scope to Agent B
delegated_identity = create_delegation(
    delegator=agent_a_identity,
    delegate=agent_b_identity,
    delegated_scopes=["read"],  # subset of delegator's scopes
)
# delegated_identity.delegation_depth == 1
# delegated_identity.delegated_by == agent_a_identity.agent_id
```

### Envelope Encryption (GDPR erasure)

```bash
pip install "ameoba[auth]"
```

```python
from ameoba.security.encryption.envelope import EnvelopeEncryption, LocalKeyProvider

kp = LocalKeyProvider()
enc = EnvelopeEncryption(key_provider=kp)

# Encrypt a record's sensitive fields
dek = await enc.get_or_create_dek(collection="patients", tenant_id="hospital-a")
ciphertext = enc.encrypt(plaintext_bytes, dek=dek)

# GDPR erasure — destroy the DEK, data is permanently unreadable
from ameoba.security.encryption.envelope import CryptographicErasure
eraser = CryptographicErasure(key_provider=kp)
await eraser.erase(collection="patients", tenant_id="hospital-a")
```

---

## Schema Registry

Ameoba automatically infers and versions the JSON Schema for every collection on each ingest. No manual schema definition needed.

### How it works

1. On every `ingest()`, Ameoba runs Spark-style map-reduce schema inference over the payload.
2. If the inferred schema differs from the latest registered version, a new version is created.
3. Compatibility is checked: `IDENTICAL` → no new version; `BACKWARD_COMPATIBLE` → new version; `BREAKING` → new version flagged.

### Schema drift detection

```python
from ameoba.schema.drift import DriftDetector

detector = DriftDetector(
    collection="events",
    window_size=100,
    on_drift=lambda collection, old_v, new_v: alert_team(collection, new_v.compatibility),
)

# Call on each record as it arrives
await detector.observe(record.payload)
```

The detector never auto-migrates. On a breaking change it fires the callback and logs a warning — the human or agent decides what to do.

### Querying schema history

```bash
# CLI: list collections
ameoba query "SELECT DISTINCT collection FROM schema_registry ORDER BY collection"

# HTTP
GET /v1/schema
GET /v1/schema/orders
GET /v1/schema/orders/versions
```

---

## Federated Query

Ameoba exposes a single SQL interface. The query planner determines whether a query touches one backend (fast path) or multiple (federation path).

### Fast path (single backend)

```sql
-- Goes directly to DuckDB
SELECT * FROM events WHERE ts > '2024-01-01' LIMIT 100;

-- Prefix-qualified (explicit backend)
SELECT * FROM pg.orders WHERE status = 'pending';
```

### Federation path (cross-backend join)

```sql
-- DuckDB join between Postgres users and Elasticsearch activity
SELECT u.name, a.title
FROM pg.users u
JOIN es.activity a ON a.user_id = u.id
WHERE u.department = 'engineering'
LIMIT 50;
```

The planner extracts tables, maps them to backends, runs sub-plans concurrently, loads results into DuckDB temporary Arrow tables, and executes the join in DuckDB's vectorized engine.

### Table-Valued Functions (TVFs)

TVFs extend SQL with backend-native operations:

**Vector search (LanceDB)**

```sql
SELECT * FROM vector_search(
    collection  := 'doc_embeddings',
    query_vector := ARRAY[0.1, 0.2, ...],
    top_k       := 10,
    filter      := 'category = ''research'''
);
```

**Full-text search (Elasticsearch)**

```sql
SELECT * FROM full_text_search(
    collection := 'articles',
    query      := 'machine learning embeddings',
    top_k      := 20,
    tenant_id  := 'acme'
);
```

**Graph traversal (Neo4j)**

```sql
SELECT * FROM graph_traverse(
    collection := 'knowledge_graph',
    start_id   := 'node-123',
    depth      := 3,
    direction  := 'outbound',
    rel_types  := 'FOLLOWS,KNOWS'
);
```

**TVF usage via kernel or HTTP:**

```python
# Via kernel query
result = await kernel.query(
    "SELECT * FROM vector_search('embeddings', ARRAY[...], 10)"
)
```

---

## Audit & Compliance

Every operation is recorded in an append-only, hash-chained SQLite ledger with an RFC 6962 Merkle tree on top.

### Audit event kinds

| Kind | When |
|---|---|
| `INGESTION` | Record received by kernel |
| `CLASSIFICATION` | Classification result recorded |
| `ROUTING` | Routing decision made |
| `WRITE` | Record written to backend |
| `READ` | Record retrieved by ID |
| `QUERY` | SQL query executed |
| `AUTH_SUCCESS` / `AUTH_FAILURE` | Authentication events |
| `SCHEMA_REGISTERED` | New schema version created |
| `SYSTEM_START` / `SYSTEM_STOP` | Kernel lifecycle |

### Tamper-evident properties

- **Append-only enforcement:** SQLite `BEFORE UPDATE/DELETE` triggers block any modification.
- **Hash chain:** Each event's hash includes the previous event's hash. A modification breaks the chain.
- **Gapless sequence numbers:** Deletions are detected by gaps in the sequence.
- **RFC 6962 Merkle tree:** O(log n) inclusion proofs without scanning the full log.

### Verifying integrity

```bash
# CLI
ameoba audit verify
# → Audit integrity: OK (1247 events, root=a3f7...)
# → Audit integrity: FAILED (gap at sequence 812)

# HTTP
GET /v1/audit/verify

# Python
ok, message = await kernel.audit_verify()
```

### Background verifier

```python
from ameoba.audit.verifier import AuditVerifier
import asyncio

async def on_tamper(message: str):
    await alert_security_team(message)

verifier = AuditVerifier(
    kernel.audit_ledger,
    interval_seconds=3600,      # check every hour
    on_failure=on_tamper,
)
task = asyncio.create_task(verifier.run())
# On shutdown: verifier.stop(); await task
```

### Export for SIEM

**OCSF (Open Cybersecurity Schema Framework):**

```python
from ameoba.audit.exporters.ocsf import to_ocsf

event = await kernel.audit_ledger.sink.tail(limit=1)[0]
ocsf_event = to_ocsf(event)
# → {"class_uid": 3001, "category_uid": 2, "severity_id": 1, ...}
```

**JSONL streaming:**

```python
from ameoba.audit.exporters.jsonl import export_jsonl

async for line in export_jsonl(kernel.audit_ledger.sink, limit=1000):
    send_to_splunk(line)
```

### S3 audit anchoring (WORM)

```python
# Anchor the current Merkle root to S3 Object Lock (7-year WORM retention)
root_hash = await kernel.audit_ledger.get_root_hash()
key = await s3_store.anchor_digest(root_hash, label="daily-audit")
print(f"Anchored at: {key}")
```

---

## gRPC API

Three services defined in `proto/ameoba/v1/`:

| Service | Methods |
|---|---|
| `IngestService` | `IngestOne` (unary), `IngestStream` (bidi-streaming) |
| `QueryService` | `Execute` (server-streaming: schema frame + data frames) |
| `AuditService` | `Tail` (server-streaming), `Verify` (unary) |

### Generate stubs

```bash
pip install "ameoba[grpc]"
bash scripts/gen_proto.sh
# → src/ameoba/api/grpc/pb/
```

### Start gRPC server

```python
from ameoba.api.grpc.server import AmeobaGRPCServer

grpc_server = AmeobaGRPCServer(kernel=kernel, port=50051)
await grpc_server.start()
await grpc_server.wait_for_termination()
await grpc_server.stop()
```

Or via CLI:

```bash
ameoba serve --grpc --grpc-port 50051
```

---

## Observability

### Structured logging (structlog)

All logs are structured JSON in production, pretty-printed in development.

```env
AMEOBA_OBS_LOG_FORMAT=json       # json | pretty
AMEOBA_OBS_LOG_LEVEL=INFO        # DEBUG | INFO | WARNING | ERROR
```

Every log line emitted during a request includes `agent_id` and `tenant_id` context vars from auth middleware.

### OpenTelemetry tracing

```bash
pip install opentelemetry-sdk opentelemetry-exporter-otlp-proto-grpc
```

```python
from ameoba.observability.tracing import configure_tracing, get_tracer

configure_tracing(
    service_name="ameoba",
    otlp_endpoint="http://localhost:4317",
)
tracer = get_tracer(__name__)

with tracer.start_as_current_span("my_operation") as span:
    span.set_attribute("collection", "events")
    # ...
```

Or via env:

```env
AMEOBA_OBS_OTLP_ENDPOINT=http://otel-collector:4317
AMEOBA_OBS_SERVICE_NAME=ameoba-prod
```

Falls back to a no-op tracer if the SDK is not installed.

### OpenTelemetry metrics

```python
from ameoba.observability.metrics import configure_metrics, get_meter

configure_metrics(
    service_name="ameoba",
    otlp_endpoint="http://localhost:4317",
)
meter = get_meter(__name__)
counter = meter.create_counter("ameoba.ingest.records_total")
counter.add(1, {"collection": "events", "category": "relational"})
```

---

## Testing

### Run the suite

```bash
# All tests
pytest

# Unit tests only
pytest tests/unit/

# Integration tests only
pytest tests/integration/

# With coverage
pytest --cov=src/ameoba --cov-report=html
open htmlcov/index.html
```

### Test coverage areas

| File | Tests | What's covered |
|---|---|---|
| `tests/unit/test_merkle.py` | 11 | RFC 6962 Merkle tree, inclusion proofs, tamper detection |
| `tests/unit/test_classifier.py` | 9 | All 4 classification layers, pipeline, category hints |
| `tests/unit/test_schema.py` | 20 | Schema inference, type widening, compatibility checks, metrics |
| `tests/unit/test_crdt.py` | 23 | GSet, ORSet, LWWRegister, HLC properties |
| `tests/unit/test_security.py` | 20 | API keys, policy engine, delegation rules |
| `tests/integration/test_embedded_ingest.py` | 8 | Real DuckDB+SQLite: ingest, query, audit, health |
| `tests/integration/test_schema_registry.py` | 9 | Schema auto-registration, versioning, drift detection |
| `tests/integration/test_staging_buffer.py` | 7 | Queue, flush, failure handling, kernel integration |

**116 tests, 0 failures.**

### Writing tests

All integration tests use the `kernel` async fixture which provides an `AmeobaKernel` wired to a temporary directory:

```python
import pytest

@pytest.mark.asyncio
async def test_my_feature(kernel):
    from ameoba.domain.record import DataRecord
    record = DataRecord(collection="test", payload={"x": 1})
    result = await kernel.ingest(record)
    assert result.classification.primary_category.value == "relational"
```

---

## Deployment

### Docker (single container, embedded mode)

```dockerfile
FROM python:3.11-slim
WORKDIR /app
COPY . .
RUN pip install ".[all]"
ENV AMEOBA_EMBEDDED_DATA_DIR=/data
ENV AMEOBA_ENVIRONMENT=production
ENV AMEOBA_OBS_LOG_FORMAT=json
VOLUME ["/data"]
EXPOSE 8000 50051
CMD ["ameoba", "serve", "--port", "8000", "--workers", "4", "--grpc"]
```

### Production checklist

- [ ] Set `AMEOBA_AUTH_JWT_SECRET` to a strong secret (RS256 + private key recommended)
- [ ] Set `AMEOBA_ENVIRONMENT=production`
- [ ] Set `AMEOBA_OBS_LOG_FORMAT=json` and point `AMEOBA_OBS_OTLP_ENDPOINT` at your collector
- [ ] Mount a persistent volume for `AMEOBA_EMBEDDED_DATA_DIR`
- [ ] Register external backends at startup (Postgres, Neo4j, etc.)
- [ ] Enable S3 Object Lock for audit anchoring (7-year WORM retention)
- [ ] Start the `AuditVerifier` background task with an alerting callback
- [ ] Configure Cedar policy grants for restricted labels (PHI, PCI, PII)
- [ ] Set `AMEOBA_AUTH_API_KEYS` to empty or disable in production (use JWT instead)

---

## Project Structure

```
ameoba/
├── proto/ameoba/v1/          # gRPC proto definitions
├── scripts/
│   └── gen_proto.sh          # Generate Python stubs from protos
├── src/ameoba/
│   ├── adapters/             # Storage backend implementations
│   │   ├── embedded/         # DuckDB, SQLite audit, local blob
│   │   ├── postgres/         # PostgreSQL (asyncpg)
│   │   ├── neo4j/            # Neo4j (async driver)
│   │   ├── elasticsearch/    # Elasticsearch/OpenSearch
│   │   ├── s3/               # S3-compatible blob (aiobotocore)
│   │   └── lancedb/          # LanceDB vector store
│   ├── api/
│   │   ├── grpc/             # gRPC server + servicers
│   │   └── http/             # FastAPI app, routers, dependencies
│   ├── audit/
│   │   ├── merkle.py         # RFC 6962 Merkle tree
│   │   ├── ledger.py         # AuditLedger coordinator
│   │   ├── verifier.py       # Background integrity verifier
│   │   └── exporters/        # OCSF + JSONL exporters
│   ├── cli/                  # Typer CLI commands
│   ├── crdt/                 # GSet, ORSet, LWWRegister, HLC
│   ├── domain/               # Pydantic domain models (frozen)
│   ├── kernel/
│   │   ├── kernel.py         # AmeobaKernel (main orchestrator)
│   │   ├── classifier/       # 4-layer classification pipeline
│   │   ├── router.py         # Routing decisions
│   │   ├── staging.py        # Staging buffer (retry queue)
│   │   └── topology.py       # Backend registry
│   ├── observability/        # Logging, tracing, metrics
│   ├── ports/                # Protocols (StorageBackend, ClassifierPlugin, ...)
│   ├── query/
│   │   ├── planner.py        # Fast path / federation path planner
│   │   ├── executor.py       # Concurrent sub-plan execution
│   │   └── tvf/              # Table-valued function handlers
│   ├── schema/               # Inference, compatibility, registry, drift
│   └── security/             # AuthN, AuthZ, delegation, encryption
└── tests/
    ├── unit/                 # Pure unit tests (no I/O)
    └── integration/          # Real DuckDB + SQLite
```

---

## License

MIT
