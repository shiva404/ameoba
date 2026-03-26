# Ameoba: Architecture Research & Reference Document
## Intelligent Adaptive Database System for Agentic Workflows

> **Purpose:** This is a research reference document, not an implementation plan. It captures architectural decisions, trade-offs, algorithms, and prior art for building an intelligent data fabric for AI agent ecosystems.

## Context

The goal is to design an **intelligent data fabric** for AI agent ecosystems — a system that acts as the authoritative "statement of record" for all data flowing through agentic workflows. It is not a database itself, but an **orchestration and routing layer** that:

- Accepts heterogeneous data (structured, unstructured, graph, files) from agents
- Automatically classifies and routes data to the appropriate storage backend
- Starts with nothing and adapts its storage topology as data demands emerge
- Maintains an immutable, auditable trail of every operation for compliance
- Provides a unified query interface across all backends

## Decisions Made

- **Deployment model:** Hybrid — embedded engines (SQLite/DuckDB) for audit, staging, and analytics; external backends (Postgres, Neo4j, etc.) for primary storage. System grows organically from embedded to external as data demands increase.
- **Primary consumers:** Both AI agents (programmatic API) and humans (CLI/query interface) are first-class citizens.
- **Scope:** This is a research/strategy document. Implementation follows after strategy is validated.

---

## Core Research Questions

### 1. Architecture Pattern: What model fits best?

**Options explored:**

| Pattern | Fit | Trade-offs |
|---|---|---|
| **Microkernel + Plugin Adapters** | High | Core handles classification/audit/routing; backends are plugins. Extensible, but plugin interface design is critical. |
| **Data Mesh / Federated** | Medium | Each domain owns its data. Good for org-level, overkill for single-system. |
| **Lakehouse (Delta/Iceberg)** | Medium | Great for analytics, but assumes tabular data. Doesn't cover graph/blob natively. |
| **Polyglot Persistence with Facade** | High | Each data type gets its ideal store; a unified API sits on top. Proven pattern but federation is hard. |

**Research direction:** Microkernel with polyglot persistence backends. The "kernel" is classification + routing + audit. Backends are swappable adapters.

---

### 2. Data Classification: How to decide where data belongs?

This is the core intelligence of the system. The classifier must inspect incoming data and decide: relational? document? graph? blob?

**Proposed pipeline (hybrid approach):**

1. **Explicit hints** — If the producer declares a type/schema, trust it (confidence: 1.0)
2. **Structural analysis (rule-based)** — Inspect shape:
   - Flat dicts with consistent keys → relational
   - Nested/irregular JSON → document
   - Nodes/edges or subject/predicate/object → graph
   - Binary/large payload → blob
   - Mixed sub-structures → decompose and re-classify parts
3. **Schema inference** — Derive JSON Schema, match against known schemas in registry
4. **Domain-specific plugins** — Custom classifiers for specific data types (medical records, financial transactions, etc.)

**Key design choice:** Rule-based + inference, NOT ML-based classification. Reasoning: routing decisions must be deterministic, auditable, and explainable for compliance. A black-box classifier would undermine the audit story.

#### Deep Dive: Classification Pipeline Architecture

**Layered cascade (cheap→expensive):**

| Layer | What it does | Cost |
|---|---|---|
| 0: Binary/Blob | Magic bytes (PNG/JPEG/PDF signatures), Shannon entropy analysis, null-byte frequency | Microseconds |
| 1: Format Detection | Is it JSON? CSV? XML? Parquet? Avro? | Sub-millisecond |
| 2: Structural Analysis | Shape inspection — flatness, key consistency, nesting depth | Milliseconds |
| 3: Semantic Classification | What the structure *means* — graph patterns, domain-specific recognition | Milliseconds-seconds |

Early exit at each layer (PNG detected at Layer 0 → skip JSON parsing).

**Structural heuristics per category:**

- **Relational:** Tabular Score = weighted sum of key consistency (Jaccard similarity > 0.85), flatness ratio (max_depth/total_fields → 1.0), type homogeneity per column, absence of arrays. Reference: Spark and BigQuery schema inference algorithms.
- **Graph:** Explicit vocabulary scan (nodes/edges/source/target/subject/predicate/object), foreign-key reference detection (values referencing other records' IDs), adjacency list patterns, triple patterns (3-element records). Requires **multiple** signals to avoid false positives from relational foreign keys.
- **Document:** Nesting depth > 2, high schema variance (Jaccard < 0.5), heterogeneous arrays, polymorphic records with type discriminators.
- **Blob:** Magic byte signatures (Apache Tika's 1400+ MIME type database), entropy > 7.0 bits/byte, null-byte frequency > 1%, base64 detection with recursive decode.

**Classification vector (not single label):**
Output is `{"relational": 0.45, "document": 0.35, "graph": 0.15, "blob": 0.05}`. Mixed data is decomposed into sub-records routed to different backends with cross-references.

**Schema inference:** Spark-style map-reduce merge — per-record schema extraction, then lattice-based type unification across records. Incremental (streaming-compatible). Versioned and append-only. Complexity score feeds back into classification.

**Plugin architecture:** Protocol-based interface with priority-ordered registry. Cascade: priority 10 (binary detector) → 20 (format) → 30 (domain plugins) → 50 (structural detectors) → 90 (fallback → document). Same-priority classifiers run in parallel, results merged via calibrated weighted soft voting.

**Edge cases resolved:**
- Mixed data → decompose with cross-references between backends
- No structure → document store (most flexible default), flag for review
- Large payloads → streaming classification with 10MB byte budget, or direct-to-blob above 1GB
- Schema drift → windowed reclassification every N records, alerts on category change, **never auto-migrate**

**Prior art:** Apache NiFi (content-based routing), Apache Atlas (classification propagation), Baazizi et al. EDBT 2017 (JSON schema inference), Souza et al. 2021 (polyglot persistence selection — recommends considering access patterns alongside data shape).

---

### 3. Storage Backend Strategy

**Per-category recommendations:**

| Category | Primary Backend | Rationale |
|---|---|---|
| **Relational** | PostgreSQL (OLTP), DuckDB (OLAP) | Postgres for transactional writes; DuckDB for analytical queries in-process |
| **Document** | Elasticsearch / OpenSearch | Full-text search, semi-structured, scales well |
| **Graph** | Neo4j | Industry standard for knowledge graphs, Cypher query language |
| **Blob/File** | S3-compatible (MinIO on-prem) | Standard, cheap, durable |
| **Audit Ledger** | PostgreSQL (dedicated, append-only) | Triggers block UPDATE/DELETE; hash-chained entries |

**Key insight (hybrid model):** Ameoba starts with embedded engines (DuckDB for relational/analytics, SQLite for audit ledger, local filesystem for blobs). As data volume or query complexity grows, it can promote storage to external backends (Postgres, Neo4j, etc.) — either self-provisioned or delegated to external agents. The **topology registry** tracks all available backends (embedded and external), and a **staging buffer** handles data awaiting backend availability during transitions.

---

### 4. Statement of Record — Audit & Compliance Design

This is non-negotiable for the use case. Every operation must be traceable.

**Design principles:**
- **Append-only ledger** — No updates or deletes, enforced at the database level (triggers + RLS)
- **Hash chain** — Each entry's hash includes the previous entry's hash (Merkle-like chain). Detects tampering.
- **Gapless sequence numbers** — Detects deletions
- **Events tracked:** ingestion, classification, routing, writes, reads, queries, schema changes, backend health changes
- **Export capability** — For external compliance systems

**Tiered approach (aligned with hybrid model):**
- **Embedded tier:** SQLite-backed hash-chained ledger. Zero infrastructure. Sufficient for development and small-scale compliance.
- **External tier:** Promote to Postgres (append-only with triggers) or Kafka for high-throughput, multi-agent environments.
- **Future consideration:** immudb or similar for regulatory environments requiring cryptographic verification. (Note: AWS QLDB was deprecated in 2024.)

#### Deep Dive: Tamper-Evident Record Keeping

**Data structure: Merkle tree over append-only log (not just hash chain)**

| Approach | Verification | Append | Tamper Detection | Best For |
|---|---|---|---|---|
| Hash chain | O(n) walk | O(1) | Pinpoints first-modified record | Small logs, simple |
| **Merkle tree** | **O(log n) inclusion proof** | **O(log n)** | **Individual record verification without full scan** | **Large-scale, long-lived logs** |
| Blockchain-style | O(log n) + O(m blocks) | O(log n) | Per-record and per-batch | Batched write systems |

**Recommendation: Merkle tree.** Billions of audit entries over years of agent workflows need O(log n) proof generation, not O(n) chain walking. Uses RFC 6962 (Certificate Transparency) construction with 0x00/0x01 leaf/internal node prefixes.

**Append-only enforcement (4 layers of defense):**

1. **Role-based:** `REVOKE UPDATE, DELETE ON audit_log FROM app_role;`
2. **Trigger (defense in depth):** `BEFORE UPDATE OR DELETE` trigger raises exception
3. **Row-Level Security:** INSERT-only policies, even for table owner
4. **Separate credentials:** Dedicated `audit_writer` role with INSERT-only permission

**Tamper detection against DBA access (the hard problem):**

- **Layer 1 — DB controls:** Stops accidental/casual modification
- **Layer 2 — Merkle tree:** Detects modification cryptographically
- **Layer 3 — External anchoring:** Publish periodic digests to tamper-proof external storage:
  - S3 Object Lock (WORM mode) — pennies, milliseconds
  - OpenTimestamps (Bitcoin anchoring) — free, ~2hr confirmation
  - Azure Confidential Ledger (SGX enclaves) — highest assurance
- **Layer 4 — Background verifier:** Separate process, different credentials, different host. Periodically re-validates Merkle tree against published digests. Alerts on discrepancy.

**Regulatory minimum viable compliance:**

| Framework | Key Requirement | Retention |
|---|---|---|
| SOC 2 | Log all access/modifications, protect from unauthorized change | 1yr min, 7yr recommended |
| HIPAA | Log all PHI access, authenticate integrity | 6 years |
| GDPR | Records of processing activities; support "right to erasure" via **cryptographic erasure** (delete encryption keys, not audit records) | Duration of processing |
| SOX | Immutable audit trail | 7 years |
| PCI-DSS | Audit trail history | 1yr (3mo immediately available) |

**Scalability: partitioned PostgreSQL with tiered storage**

| Tier | Age | Storage | Access |
|---|---|---|---|
| Hot | 0-3 months | Postgres + NVMe | Full SQL, indexed |
| Warm | 3-12 months | Postgres read replicas | SQL, slower |
| Cold | 1-7 years | Parquet on S3 (queryable via DuckDB/Athena) | Rare compliance queries |
| Archive | 7+ years | S3 Glacier | Subpoena response |

Use `pg_partman` for automated monthly partition management. Detach old partitions instantly regardless of size.

**Export formats:** OCSF (Open Cybersecurity Schema Framework — emerging standard, backed by AWS/Splunk/IBM), CEF (legacy, widely supported), JSONL (universal). SIEM integration via Splunk HEC, Elastic Filebeat, or Amazon Security Lake.

---

### 5. Unified Query Across Heterogeneous Backends

This is the hardest problem. How do you query across Postgres + Neo4j + Elasticsearch in a single request?

#### Deep Dive: Federated Query Engine

**Query language decision: SQL with table-valued functions (not custom DSL)**

Standard SQL as the base, with backend-specific capabilities exposed through table-valued functions. This avoids building a custom parser while enabling full power of each backend:

```sql
-- Relational (fast path → Postgres)
SELECT u.name FROM pg.users u WHERE u.active = true;

-- Graph traversal via table-valued function
SELECT u.name, f.friend_name
FROM pg.users u
JOIN neo4j.traverse('User', u.id, 'FOLLOWS', 3) AS f ON true;

-- Full-text search via table-valued function
SELECT d.title, d.score
FROM es.search('documents', 'federated query', top_k := 20) AS d;

-- Cross-backend join (federation path → DuckDB computes the join)
SELECT u.name, d.title
FROM pg.users u
JOIN es.search('activity', u.name) AS d ON d.user_id = u.id;
```

Why SQL: universal familiarity, mature tooling (BI tools, JDBC), DuckDB natively speaks it (no translation step), Trino/Calcite/Dremio all validate this approach.

**Two-level query planning:**

- **Fast path (single backend):** Translate logical plan directly to native query (SQL, Cypher, ES DSL). Zero federation overhead. This is the common case.
- **Federation path (multi-backend):** Decompose logical plan into per-backend sub-plans, push down predicates/projections/aggregations per backend's capability manifest, execute concurrently, join results in DuckDB.

**Pushdown capabilities by backend:**

| Backend | Predicates | Projection | Aggregation | Joins | Sort | Limit |
|---|---|---|---|---|---|---|
| PostgreSQL | Full SQL | Yes | Full | Yes (internal) | Yes | Yes |
| Neo4j | Property filters | Yes | Partial (count, collect) | N/A (graph patterns) | Yes | Yes |
| Elasticsearch | Filters, full-text | Yes | Partial (bucket/metric aggs) | No | Yes | Yes |
| S3/Parquet | Row group statistics | Column pruning | No | No | No | No |
| DuckDB | Full SQL | Yes | Full | Yes | Yes | Yes |

**Cross-backend join strategies (cost-model selects):**

1. **Batched nested loop** — outer < 100 rows, inner is indexed. Batch IDs into single query.
2. **Hash join in DuckDB** — default/safest. Both sides loaded as Arrow-backed temp tables. DuckDB's vectorized engine handles the join.
3. **Semi-join with Bloom filter** — one side much larger. Extract keys from small side, push as `IN (...)` filter to large side. Up to 46x speedup per CIDR 2024 research.

Decision heuristic: nested loop if outer < 100 rows, semi-join if Bloom filter selectivity < 0.1, hash join otherwise. Requires statistics from the catalog (row counts, cardinality).

**DuckDB as federation compute engine:**
- In-process, zero network overhead
- Columnar vectorized execution (hash joins on 10M rows in sub-second)
- Apache Arrow zero-copy integration (backends → Arrow → DuckDB temp tables)
- Native Parquet/S3 reading via `httpfs` extension (S3 backend gets a free query engine)
- Already has `postgres_scanner` extension proving the architecture

**Consistency model: snapshot-approximate**
- Each backend provides its best consistent snapshot, but no global snapshot guarantee
- ES lags ~1s (configurable `_refresh` before query for freshness)
- Postgres uses `REPEATABLE READ` for sub-query consistency
- No distributed transactions — accept eventual consistency, document clearly
- Partial failures: fail-fast for joins (incomplete join = wrong results), graceful degradation for UNION-like gathers
- Circuit breaker per backend to prevent cascade failures

**Federated pagination:**
- Single-backend: delegate (OFFSET/LIMIT, cursor-based, search_after)
- Multi-backend: server-side cursor backed by DuckDB temp table. Trade memory for correctness.

**Prior art borrowed from:** Trino (connector SPI design), Calcite (trait-based calling conventions), Dremio (materialized reflections for hot queries), DuckDB (The Great Federator — MotherDuck blog).

---

### 6. Schema Evolution in an Append-Only World

Data shapes change over time. How to handle this when nothing is mutable?

**Approach: Versioned schema registry**
- Every schema version is immutable and stored
- Records reference their schema version
- New versions are registered on structural change detection
- Compatibility is tracked: backward-compatible (additive fields) vs. breaking changes
- Query engine handles version-aware deserialization — can query across schema versions

---

### 7. Data Lifecycle: Raw → Intermediate → Final

Agents produce data at different stages. The system must track lifecycle:

- **Raw** — Unprocessed input (uploaded files, API responses, scrapes)
- **Intermediate** — Partially processed (extracted entities, parsed structures)
- **Final** — Authoritative output (reports, decisions, summaries)

Lifecycle is metadata on the DataRecord, not a separate storage concern. But it informs retention policies and query relevance.

---

### 8. CRDTs (Conflict-Free Replicated Data Types) for Record Keeping

CRDTs enable replicas to be updated independently and always merge to a consistent state without coordination. Key property: merge function is commutative, associative, and idempotent → replicas always converge.

**Where CRDTs fit in Ameoba:**

| Component | CRDT Type | Purpose | Why CRDT (not consensus) |
|---|---|---|---|
| Audit event collection | **G-Set** | Partition-tolerant capture — no events lost during network splits | Append-only ∩ commutative union = perfect G-Set fit |
| Cluster topology | **OR-Set + LWW-Register** | Node membership + health status | Stale-by-seconds is acceptable for health |
| Schema registry | **OR-Set (fields) + MV-Register (properties)** | Concurrent schema evolution from multiple agents | Non-conflicting field additions merge automatically |
| Data classification | **OR-Set of (agent, label, timestamp)** | Multi-agent classification results | All classifications preserved; read-time policy resolves |
| Record metadata | **LWW-Register** | Lifecycle state, ownership | Latest observation wins |

**Where CRDTs do NOT fit (use consensus instead):**

| Component | Mechanism | Why |
|---|---|---|
| Audit log **ordering** | Raft sequencer | Total order required for compliance hash chain |
| Record **writes** | Raft consensus | "Statement of record" needs linearizable mutations |
| Access control | Single-leader | Authorization must not have false positives from stale data |

**The ordering problem and reconciliation:**

CRDTs guarantee convergence but not ordering. Audit trails require total order. Solution: **hybrid architecture**.

- **Normal operation:** Events flow through Raft-based sequencer → immediate total ordering → hash chain/Merkle tree computed on ordered log.
- **During network partition:** G-Set CRDT captures events locally (no events lost). After partition heals, un-sequenced events get sequence numbers and integrate into canonical log. Merkle tree recomputed incrementally.
- **Timestamps:** Hybrid Logical Clocks (HLC) — combines wall-clock + logical counter for causal ordering of concurrent events.

**Schema registry with CRDTs:**
Two agents concurrently add different fields → OR-Set merges both (conflict-free). Two agents change same field's type → MV-Register preserves both values, surfaces conflict for resolution. Enforcement: backward-compatible-only changes (additive fields, widening types) makes most concurrent evolution conflict-free by construction.

**Key prior art:**
- **Riak:** Production CRDTs (counters, sets, maps). Lesson: sibling resolution (MV-Register conflicts) is real operational burden.
- **Automerge:** JSON-document CRDT with efficient delta-sync protocol. Relevant for complex record structures.
- **SoundCloud Roshi:** LWW-element-set on Redis sorted sets for timestamped events at massive scale. Architecture: stateless CRDT logic atop durable storage — maps to Ameoba's model.
- **Log-Structured CRDTs (UCSB research):** Integrates append-only logging into CRDT model. Provides version history, state reconstruction at any point, 1.8x better throughput than delta-CRDTs. Most directly applicable prior art.

**Combined architecture:**

```
Agent writes → Raft Sequencer (strong consistency for ordering)
                    ↓
    Ordered Audit Log (append-only, Merkle tree, partitioned)
                    ↓
    External Anchoring (S3 Object Lock / OpenTimestamps)

During partition:
    Local G-Set CRDT → captures events → merges post-heal → sequenced into canonical log
```

---

### 9. Security, Access Control & Agent Identity

#### Authentication: Layered Stack

| Mechanism | Use Case | Priority |
|---|---|---|
| **OAuth2 Client Credentials + Private Key JWT** | Production agents (RFC 7523) | P0 |
| **mTLS** | Infrastructure-level trust, high-security agents | P1 |
| **API Keys** | Development/testing only (scoped, rate-limited) | P0 |
| **OIDC Authorization Code** | Human users via identity providers | P1 |

Follows emerging IETF draft for OAuth 2.0 AI Agent On-Behalf-Of Authorization.

#### Agent Identity Hierarchy

```
Organization (tenant boundary, data isolation)
  └── Agent Group (shared policies, e.g., "analytics-agents")
       └── Agent Identity (unique principal, primary accountability unit)
            └── Agent Session (ephemeral, per-workflow, audit correlation key)
```

#### Agent-to-Agent Delegation

RFC 8693 Token Exchange with `act` claim: Agent A delegates to Agent B → delegation token embeds both identities. Effective permissions = intersection(A's delegated scope, B's own permissions). Max delegation depth: 3 levels.

#### Authorization: Cedar Policy Engine (PBAC + ReBAC hybrid)

Why Cedar over OPA: 42-60x faster policy evaluation (critical for query hot path), formal verification support (provable correctness for compliance), native relationship support (ReBAC without separate infrastructure).

**Authorization Gateway Pattern** — enforced at Ameoba layer, before queries reach backends:
- Backends use service accounts with broad permissions
- Cedar policies evaluate caller identity + resource classification + operation type
- For reads: inject filters (WHERE clauses for PG, term filters for ES)
- For cross-backend queries: each backend leg independently authorized
- Policy changes are themselves audit events

**Permission granularity (4 levels):**
1. Per-backend (can this agent access Postgres?)
2. Per-collection (can this agent read `patients`?)
3. Per-record (row-level filtering via query rewriting)
4. Per-field (column masking/redaction)

#### Encryption

**At rest — Envelope encryption:**
```
Cloud KMS (Master Key) → wraps → Tenant KEK → wraps → Data DEK
```
- Default: per-tenant KEK + per-collection DEK
- GDPR mode: per-data-subject DEK → cryptographic erasure by destroying DEK
- Key rotation: 90-day cycle, re-wraps DEK with new KEK (no data re-encryption)

**In transit:** TLS 1.3 mandatory everywhere. Postgres `sslmode=verify-full`, Neo4j Bolt+TLS, ES HTTPS, S3 HTTPS. Optional mTLS for high-security agents. SPIFFE/SPIRE for Kubernetes workload identity.

#### Data Classification Labels

| Label | Example | Access Implication |
|---|---|---|
| `PUBLIC` | Product catalog | Any authenticated agent |
| `INTERNAL` | Internal metrics | Org-member agents only |
| `CONFIDENTIAL` | Financial data | Named agents/groups only |
| `PII` | Email, name | GDPR-scoped access + audit |
| `PHI` | Medical records | HIPAA-authorized only |
| `PCI` | Credit card numbers | PCI-DSS compliant access |

Labels are additive, inherited (collection → records), and enforced via Cedar policies. Cross-backend joins inherit the most restrictive label. Automated detection via regex patterns (Luhn for CC, format for SSN) + schema-name inference (`ssn` → PII).

#### Multi-Tenancy

Tiered isolation aligned with hybrid deployment:
- **Embedded mode:** Single-tenant, no isolation needed
- **Shared mode:** Schema-per-tenant (PG/DuckDB), tenant-filtered queries (ES), tenant-labeled nodes (Neo4j), prefix-based (S3)
- **Dedicated mode:** Database-per-tenant, separate indices/instances, dedicated S3 buckets

Every query auto-injects `tenant_id` from authenticated token (never from user input). Defense-in-depth: PG Row-Level Security as second enforcement layer. Per-tenant connection pools, rate limits, query timeouts, storage quotas.

**Prior art:** Databricks Unity Catalog (unified governance across compute engines), Snowflake Horizon (federated catalog). Key difference: Ameoba routes to external backends it doesn't control → authorization gateway is non-negotiable.

---

### 10. Vector/Embedding Storage

#### Tiered Vector Architecture (aligned with hybrid model)

| Tier | Scale | Backend | Integration |
|---|---|---|---|
| **0: Embedded (small)** | < 1M vectors | DuckDB VSS extension | Native — compute layer does vector ops directly |
| **1: Embedded (medium)** | 1M-100M | LanceDB | Arrow zero-copy ↔ DuckDB, S3-backed storage, 1.5M IOPS |
| **2: Co-located** | varies | pgvector / ES kNN / Neo4j vector index | Vectors alongside their source data (relational/document/graph) |
| **3: Dedicated (large)** | 100M+ | Qdrant or Milvus | Dedicated vector infrastructure |

#### Vector Search via SQL Table-Valued Functions

```sql
-- Similarity search
SELECT * FROM vector_search('doc_embeddings', embed('machine learning'), top_k := 10);

-- Cross-backend: vector + relational join
SELECT vs.content, vs.score, u.name
FROM vector_search('doc_embeddings', embed('ML papers'), top_k := 20) AS vs
JOIN pg.users u ON vs.author_id = u.id
WHERE u.department = 'research';

-- Hybrid: BM25 + vector fusion
SELECT * FROM hybrid_search('articles', text_query := 'neural nets',
    vector_query := embed('deep learning'), alpha := 0.7, top_k := 10);
```

Query plan for cross-backend vector+metadata: push ANN to vector backend (returns candidates) → push metadata filters to relational/document backends using candidate IDs → join in DuckDB → final ranking + LIMIT.

#### Classifier Extension

New `VECTOR` category. Detection signals:
- Fixed-length float arrays of typical embedding dimensions (384, 768, 1024, 1536, 3072)
- Field names: `embedding`, `vector`, `emb`, `representation`
- Sub-routing: standalone vectors → LanceDB; vectors with relational data → pgvector; vectors with documents → ES kNN

#### Embedding Management

- **Store-only by default**, optional `embed()` function calling configured providers (OpenAI, Cohere, Ollama/vLLM)
- **Versioning:** Each collection tracks `model_id`, `model_version`, `dimensions`, `distance_metric`. Model change → blue-green collection swap or lazy re-embedding
- **Multi-modal:** Text (384-3072d), image (512-768d, CLIP), code (768-1024d). Collections must be homogeneous in dimensionality
- **Lifecycle:** Staleness detection (`source_modified_at` vs `embedding_created_at`), cascading deletes (GDPR: deleting source must delete embeddings), provenance tracking (which model + version + input → this embedding)

#### Quantization Decision

| Technique | Compression | Recall Impact | When |
|---|---|---|---|
| None (FP32) | 1x | Baseline | < 1M vectors, accuracy-critical |
| Scalar (INT8) | 4x | < 1% loss | **Default for most workloads** |
| Binary (1-bit) + rescore | 32x → FP32 top-k | < 2% loss | 1536+ dimensions at scale |

#### GDPR for Embeddings

Embeddings can be partially inverted → they are personal data under GDPR. Requirements: PII detection before embedding, cascading deletes to all vector indices, deletion certificates, separate collections for high-sensitivity data with shorter retention.

**Prior art:** Mem0 (dual vector + graph agent memory — strong Ameoba use case), LangChain (VectorStore abstraction over 50+ backends), LlamaIndex (query routing to best index), agentic RAG (iterative multi-step vector retrieval).

---

## Technology Stack (Recommended)

- **Language:** Python 3.11+ (async-first, Pydantic v2 for models)
- **API:** FastAPI (HTTP) + gRPC (agent-to-agent)
- **CLI:** Typer
- **ORM/DB:** SQLAlchemy 2.0 + asyncpg
- **Backends:** PostgreSQL, Neo4j, Elasticsearch, S3/MinIO, DuckDB, LanceDB
- **Auth:** Cedar policy engine, OAuth2/OIDC, envelope encryption via KMS
- **Vector:** LanceDB (primary), pgvector/ES kNN (co-located), DuckDB VSS (compute)
- **Testing:** pytest + testcontainers (real backends in Docker)
- **Observability:** OpenTelemetry + structlog

---

## Key Risks & Remaining Open Questions

**Resolved through deep dives (5 rounds):**
- Classification → layered cascade, decomposition, streaming byte budget, schema drift alerts
- Query federation → DuckDB compute engine, cost-model join strategies, SQL + TVFs
- Audit → Merkle tree + external anchoring + background verification
- CRDTs → hybrid (CRDTs for availability, Raft for ordering/writes)
- Security → Cedar policy engine, OAuth2 + delegation tokens, envelope encryption, tiered multi-tenancy
- Vectors → tiered LanceDB/pgvector/DuckDB VSS, SQL TVFs for similarity search, GDPR embedding compliance

**Still open:**
1. **Provisioning protocol** — Exact interface between Ameoba and external provisioning agents. Webhook? gRPC? Staging buffer design during provisioning delays?
2. **Operational concerns** — Monitoring, alerting, runbooks for a system with 6+ backend types. How to keep operational complexity manageable?
3. **Migration paths** — How to migrate data between backends as needs change (e.g., moving from embedded DuckDB to external Postgres)?

---

## Verification (when implementation begins)

- Ingest heterogeneous data via CLI → verify correct classification and routing
- Query across multiple backends → verify federated results are correct
- Run `audit verify` → confirm Merkle tree integrity
- Simulate backend unavailability → verify staging buffer and recovery
- Schema evolution test → ingest data with changed shape, query across versions
- Vector search → ingest embeddings, run similarity search, cross-backend vector+metadata join
- Security → agent authentication, Cedar policy enforcement, field-level redaction
- GDPR → cryptographic erasure (destroy DEK, verify data unreadable), embedding cascade delete
- Multi-tenancy → verify tenant isolation (agent in tenant A cannot see tenant B's data)

---

## Key Research Sources

- **Classification:** Baazizi et al. EDBT 2017 (JSON schema inference), Souza et al. 2021 (polyglot persistence selection), Apache NiFi/Atlas
- **Federation:** Trino connector SPI, Apache Calcite (arXiv:1802.10233), CIDR 2024 predicate transfer, MotherDuck "DuckDB The Great Federator"
- **Audit:** RFC 6962 (Certificate Transparency Merkle trees), Cossack Labs (cryptographic audit logs), VLDB 2004 (tamper detection)
- **CRDTs:** crdt.tech, UCSB Log-Structured CRDTs, SoundCloud Roshi, Automerge
- **Security:** IETF draft OAuth2 AI Agent OBO, Cedar policy language, CyberArk zero-trust agents, NIST 800-53/1800-35
- **Vectors:** LanceDB+DuckDB integration, pgvector 0.8.0 benchmarks, DiskANN (Microsoft Research), Mem0 architecture
