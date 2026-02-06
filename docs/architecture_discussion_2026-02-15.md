# Backparq Architecture Discussion — 2026-02-15

> A candid code review and architecture redesign discussion.

---

## Part 1: Senior Engineer Review of Backparq v1

### Rating: 5/10 — "Interesting idea, rough execution"

#### 🔴 Problems Found

1. **README oversells** — "Data Lake" is aggressive for what is essentially `SELECT * → Parquet → S3 upload`. A Data Lake implies catalog, schema evolution, partitioning strategy, ACID transactions.

2. **Monolithic procedural architecture** — `archive.py` is 674 lines. `_process_chunk_impl` alone is 165 lines with interleaved concerns: manifest checking, export, upload, SHA verification, deletion — all in one function. No separation of concerns.

3. **Global mutable state** — Module-level `_current_result = ArchiveResult()` with a threading lock. Not reentrant, not testable in parallel. Functions mutate via side effects instead of returning values.

4. **Mid-file imports everywhere** — Imports at lines 45-50, again at line 430, again at 548 in `archive.py`. Circular import workarounds baked in. Module graph is tangled.

5. **`psycopg2-binary` in 2026** — psycopg2 is in maintenance mode. psycopg3 has native async, connection pooling, pipeline mode, COPY with binary format.

6. **Homegrown connection pool** — Why not use psycopg3's built-in `ConnectionPool` or even `psycopg2.pool.ThreadedConnectionPool`?

7. **Naive hardcoded monthly chunking** — `list_chunks` always generates monthly chunks. No adaptive sizing. 500M rows/month = one giant file. 10 rows/month = thousands of tiny files (small file problem).

8. **Broken import path** — `delete_chunk_with_verification` imports from `backparq.s3` (doesn't exist). Actual module is `backparq.storage.s3`. Dead code path that crashes at runtime in offload mode.

9. **Thin test suite** — One integration test with 3 rows. No edge cases tested.

10. **Signature mismatch** — `vacuum_table` in `operations.py` takes `DatabaseConfig`, but `archive.py` calls it with a connection from the pool. Will crash if vacuum is enabled.

11. **`Any` types everywhere** — `config: Any`, `pool: Any`, `s3: Any`, `conn: Any` — defeats the purpose of mypy strict mode.

12. **Dead Python support** — `requires-python = ">=3.8"` but Python 3.8 has been EOL since October 2024.

#### 🟢 What's Good

1. **The idea is genuinely useful** — Postgres → Parquet → S3 as a single CLI is valuable.
2. **SHA256 verification before delete** — Safety-first design for offload mode is correct.
3. **Well-documented YAML config** — `reference.yaml` at 364 lines with inline docs.
4. **Streaming export with server-side cursors** — Correct approach for large tables.
5. **Data masking** (hash, redact, partial) — Differentiating feature that WAL-G doesn't have.

---

## Part 2: Who Will Use This?

### The Real Audience

**Small-to-medium teams (5-50 engineers) with a growing PostgreSQL that's becoming painful, but no dedicated data engineering team.**

| Scenario | Pain Point | Backparq Solution |
|----------|-----------|-------------------|
| SaaS startup, Series A-B | `events` table is 200GB, queries slow, RDS bill climbing | Offload events older than 90 days to S3 |
| Fintech compliance | Must retain 7 years of transactions | Archive with column masking (PII hashed) |
| B2B platform | Customer asks "restore my data from last Tuesday" | Surgical restore of specific tables/date ranges |
| Team with no Airflow | They use cron jobs, need something that "just works" | `crontab -e` + backparq |

### The Moat

Export is easy. The **round-trip** — export, verify, delete, and bring it back months later into a database whose schema has evolved — that's hard. That's where Backparq should be unbeatable.

---

## Part 3: Proposed Architecture (v2)

### Core Insight

Every operation is a **state transition on a chunk of data**:

```
IN_DB → EXPORTED → UPLOADED → OFFLOADED
                                  ↓
                              (restore)
                                  ↓
                               IN_DB
```

### Layered Architecture

```
Layer 4: CLI          backparq archive --config ...
Layer 3: Orchestrator  Pipeline(export → upload → delete)
Layer 2: Operations    export_chunk(), upload_chunk(), delete_chunk(), restore_chunk()
Layer 1: Adapters      PostgresAdapter, S3Adapter, Catalog (SQLite)
Layer 0: Primitives    chunking, checksum, masking, serialization
```

Each layer only talks to the one below it. No skipping. No circular imports.

### Key Architectural Decisions

1. **SQLite catalog replaces JSON manifests** — Single `backparq.db` tracks all chunks, states, checksums, run history. Gives resumability, queryable history, atomic state transitions.

2. **State machine enforces safety** — Can't delete data that hasn't been verified on S3. Not because of if-statements, but because the state machine cannot transition without passing through UPLOADED.

3. **Pipeline architecture** — While chunk N uploads (I/O bound), chunk N+1 exports (CPU/DB bound). Each stage is independent.

4. **Pydantic v2 for config** — Replaces 600 lines of manual parsing with 80 lines of model definitions. Gets validation, env var expansion, JSON schema generation for free.

5. **psycopg3** — Native async, built-in connection pooling, binary COPY, pipeline mode.

6. **Adaptive chunking** — Target configurable file size (e.g., 256MB) instead of hardcoded monthly chunks.

7. **Vectorized masking** — Operate on Arrow columns instead of Python dicts for 100x speedup.

### What This Gives Us

| Concern | v1 (Current) | v2 (Proposed) |
|---------|-------------|---------------|
| Safety | SHA256 check buried in 165-line function | State machine — can't delete without UPLOADED state |
| Resumability | Re-scan all manifests on disk | Query SQLite: `WHERE state < target_state` |
| Testability | Mock everything, test nothing | Each layer testable in isolation |
| Concurrency | Global mutable lock + threading | Pipeline with async stages, no shared state |
| Extensibility | Edit archive.py monolith | Add new adapter without touching operations |
| Observability | JSON progress file | `backparq status`, `backparq history` |

---

*This document is a snapshot of the architecture discussion. See `implementation_plan.md` for the detailed execution plan.*
