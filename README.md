# Backparq

**Turn your PostgreSQL backups into a queryable Data Lake.**

Backparq is a specialized tool with a singular mission: **bridge the gap between transactional databases and analytical storage.**

Unlike traditional backup tools that lock data into opaque binary formats, Backparq exports your tables as **Parquet files on S3**. This means your backups aren't just an insurance policy—they are an accessible, queryable asset.

[![CI](https://github.com/hemashoe/backparq/actions/workflows/ci.yml/badge.svg)](https://github.com/hemashoe/backparq/actions/workflows/ci.yml)
[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/backparq.svg)](https://pypi.org/project/backparq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

See [CONTRIBUTING.md](CONTRIBUTING.md).

## Code of Conduct
Please note that this project is released with a [Code of Conduct](CODE_OF_CONDUCT.md). By participating in this project you agree to abide by its terms.

## Mission

Backparq makes backups useful immediately, not just in emergencies.

- **Don't just store it, query it**: Use DuckDB, Athena, Spark, or Pandas to run analytics directly on your S3 archives without restoring them.
- **Offload, don't just delete**: When the database grows too large, move historical data to cheap S3 storage while keeping it queryable.
- **Surgical precision**: Restore exactly the tables and rows you need (e.g., "Users table from last Tuesday"), rather than rolling back the entire database.

## The Right Tool for the Job

Backparq is **not** a replacement for WAL-G or pgBackRest. It is a complementary tool designed for different use cases.

| Feature | **WAL-G / pgBackRest** | **Backparq** |
|---------|-----------------------|--------------|
| **Core Philosophy** | **Disaster Recovery** (The "Red Button") | **Data Portability & Archival** |
| **Analogy** | A security camera recording (continuous video) | A professional photoshoot (high-quality snapshots) |
| **Format** | Opaque binary (WAL segments) | **Open Standard (Parquet)** |
| **Queryable?** | No (must restore DB first) | **Yes (directly on S3)** |
| **Granularity** | Entire Database only | **Specific Tables / Date Ranges** |
| **Best For** | Point-in-Time Recovery to a specific second | Analytics, Long-term Archival, Partial Restores |

**The Perfect Setup:**
1. Use **WAL-G** for your disaster recovery safety net (RPO ≈ 0).
2. Use **Backparq** to offload historical data and create queryable snapshots for analytics.

## Features

- **Columnar Efficiency**: Parquet + Snappy/Zstd compression reduces storage costs significantly compared to raw SQL or CSV.
- **Smart Offloading**: Archive data older than X days and optionally delete it from PostgreSQL to reclaim space.
- **Safety First**: SHA256 checksums are verified for every single file before any data is deleted.
- **Streaming & Parallel**: Uses server-side cursors and parallel uploads to handle varying table sizes efficiently without blowing up memory.
- **Serverless Friendly**: Runs anywhere (Kubernetes cronjob, Lambda, EC2 micro) and talks directly to S3.

## Installation

```bash
# Recommended: use uv for fast, reliable management
curl -LsSf https://astral.sh/uv/install.sh | sh

# Install backparq
uv pip install backparq

# With DuckDB for querying archives directly
uv pip install backparq[query]
```

## Quick Start

```bash
# 1. Initialize configuration
backparq init

# 2. Dry run (see what would happen without touching data)
backparq archive --config backparq.yaml --dry-run

# 3. Seamless Offload (Archive + Delete from DB)
backparq archive --config backparq.yaml --stats
```

## Use Cases

### 1. The "Bottomless" Database (Offloading)
Keep your primary PostgreSQL lean and fast by moving historical data to S3.

```yaml
# offload.yaml
archive:
  mode: offload
  cutoff: "-1y"          # Move everything older than 1 year
  perform_delete: true   # Delete from Postgres after safe upload
  tables:
    - public.logs
    - public.audit_trails
    - public.events
```

### 2. Analytics-Ready Snapshots
Take nightly snapshots of critical tables for your data science team.

```yaml
# backup.yaml
archive:
  mode: backup
  tables:
    - public.users
    - public.transactions
    - public.products
```

### 3. Surgical Restore
Did someone accidentally delete rows from `users`? Restore just that table.

```bash
backparq restore --config backparq.yaml \
  --tables public.users \
  --start 2024-01-01 --end 2024-01-02 \
  --conflict-mode upsert
```

## Integrations

Since Backparq uses standard Parquet, your backups integrate instantly with the modern data stack:

- **DuckDB**: `SELECT * FROM 's3://bucket/*.parquet'`
- **AWS Athena**: Create external tables on top of your backup prefixes.
- **Spark / Databricks**: Native read support.
- **Pandas**: `pd.read_parquet("s3://...")`

## Scheduling

Backparq is designed to run non-interactively, making it perfect for cron jobs.

To run a backup every night at 3 AM:

```bash
# Open crontab
crontab -e

# Add line (ensure full paths or activate venv)
0 3 * * * /path/to/venv/bin/backparq archive --config /path/to/backup.yaml --stats >> /var/log/backparq.log 2>&1
```

## Configuration

See [`examples/reference.yaml`](examples/reference.yaml) for the complete configuration reference, including encryption, compression settings, and concurrency tuning.

## Contributing

Contributions are welcome! Please use `uv` for development:
```bash
uv venv && source .venv/bin/activate
uv pip install -e ".[dev]"
uv run pytest
```

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

MIT