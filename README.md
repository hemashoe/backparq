# Backparq

**Table-level PostgreSQL Backup to Parquet on S3.**

Backparq exports PostgreSQL tables as Parquet files to S3, enabling fast restores, columnar analytics, and long-term retention at a fraction of the cost of keeping data in your database.

[![CI](https://github.com/hemashoe/backparq/actions/workflows/ci.yml/badge.svg)](https://github.com/hemashoe/backparq/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/backparq.svg)](https://pypi.org/project/backparq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Why Backparq?

| Problem | How Backparq Solves It |
|---------|------------------------|
| **Database growing too large** | Archive old data to S3, optionally delete from PostgreSQL |
| **Need table-level backups** | Full or incremental snapshots per table, not whole database |
| **Want to query historical data** | Parquet format works with DuckDB, Athena, Spark, Pandas |
| **Slow restores from pg_dump** | Restore specific tables and date ranges in minutes |
| **No visibility into backups** | SHA256 checksums, verification commands, progress bars |

## Backparq vs WAL-G vs pg_dump

| | **pg_dump** | **WAL-G** | **Backparq** |
|---|-------------|-----------|--------------|
| **Backup Scope** | Full database | Full database (WAL + base) | Per-table, selectable |
| **Backup Format** | SQL / custom binary | WAL segments + base backup | Parquet (columnar) |
| **Incremental** | No | Yes (WAL streaming) | Yes (by date ranges) |
| **Restore Granularity** | Entire DB or single table | Point-in-time (whole DB) | Per-table + date range |
| **Query Backups Directly** | No | No | Yes (DuckDB/Athena/Spark) |
| **Storage Efficiency** | Low (uncompressed SQL) | Medium | High (columnar + zstd) |
| **S3 Native** | Via pipe/script | Yes | Yes |
| **Use Case** | Migrations, full dumps | Continuous DR, PITR | Table archival, analytics-ready backups |

### When to Use Each

- **pg_dump**: One-off migrations, development snapshots, schema exports
- **WAL-G**: Continuous disaster recovery, point-in-time recovery to any second
- **Backparq**: Table-level backups, cold data archival, analytics on historical data

**Best practice**: Use WAL-G for continuous DR + Backparq for table-level archival and analytics.

## Features

- **Two Modes**: Backup (full snapshots) or Offload (archive + delete old data)
- **Table Selection**: Choose specific tables, not the entire database
- **Date-Range Restore**: Restore only the data you need, not everything
- **Columnar Format**: Query backups directly with DuckDB, Athena, or Spark
- **Parallel Processing**: Concurrent table and chunk processing
- **Streaming Export**: Constant memory usage regardless of table size
- **Safety First**: SHA256 checksums verified before any deletion
- **Encryption**: S3 SSE-S3, SSE-KMS, or client-side Parquet encryption

## Installation

```bash
# Recommended: use uv
curl -LsSf https://astral.sh/uv/install.sh | sh
uv pip install backparq

# Or pip
pip install backparq

# With DuckDB for querying archives
uv pip install backparq[query]
```

## Quick Start

```bash
# 1. Generate config
backparq init

# 2. Test connections
backparq test --config backparq.yaml

# 3. Run backup (preview)
backparq archive --config backparq.yaml --dry-run

# 4. Run backup
backparq archive --config backparq.yaml --stats
```

## Usage

### Backup Tables

Create full snapshots of specific tables:

```bash
backparq archive --config backup.yaml --stats
```

```yaml
# backup.yaml
archive:
  mode: backup           # Full snapshot, no deletion
  tables:
    - public.users
    - public.orders
    - public.transactions
```

### Archive + Delete Old Data

Move data older than 90 days to S3 and reclaim database space:

```bash
backparq archive --config offload.yaml --stats
```

```yaml
# offload.yaml
archive:
  mode: offload
  cutoff: "-90d"         # Archive data older than 90 days
  perform_delete: true   # Delete after verified S3 upload
  tables:
    - public.events
    - public.audit_logs
```

### Restore

```bash
# Restore specific date range
backparq restore --config restore.yaml \
  --start 2024-01-01 --end 2024-03-01

# Restore from a specific backup snapshot
backparq restore --config restore.yaml \
  --backup-id 2024-01-15_120000

# Update existing rows with archived data
backparq restore ... --conflict-mode upsert
```

### Query Archives Directly

```bash
# Query with DuckDB (requires backparq[query])
backparq query --config backparq.yaml \
  --sql "SELECT COUNT(*) FROM public_orders WHERE created_at > '2024-01-01'"
```

### Verify & Maintain

```bash
backparq check --config backparq.yaml   # List archives
backparq verify --config backparq.yaml  # Verify checksums
backparq prune --config backparq.yaml   # Delete old backups per retention
```

## Configuration

```yaml
database:
  host: localhost
  name: production_db
  user: postgres
  password: "${PG_PASSWORD}"

s3:
  bucket: my-backups
  prefix: postgres
  region: us-east-1
  sse: "AES256"  # Server-side encryption

archive:
  mode: backup   # or "offload"
  tables:
    - table: public.orders
      primary_key: order_id
```

See [`examples/reference.yaml`](examples/reference.yaml) for all options.

## Example Configs

| File | Use Case |
|------|----------|
| [`examples/backup.yaml`](examples/backup.yaml) | Full table snapshots for DR |
| [`examples/offload.yaml`](examples/offload.yaml) | Archive old data + delete from DB |
| [`examples/restore.yaml`](examples/restore.yaml) | Restore with date ranges |
| [`examples/reference.yaml`](examples/reference.yaml) | All configuration options |

## Security

- **Credentials**: IAM roles, environment variables, or AWS credentials file
- **Encryption at rest**: S3 SSE-S3, SSE-KMS, or client-side Parquet encryption
- **Encryption in transit**: HTTPS to S3
- **Checksums**: SHA256 verification before any deletion

See [SECURITY.md](SECURITY.md) for vulnerability reporting.

## Contributing

```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
uv run pytest
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT
