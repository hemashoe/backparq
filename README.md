# Backparq

**Production-grade PostgreSQL Archival and Backup Tool.**

Backparq efficiently archives PostgreSQL tables to Parquet files on S3. It is designed for high-performance data offloading (archiving) and full disaster recovery backups, with a strong emphasis on data safety, verification, and ease of use.

[![Python 3.8+](https://img.shields.io/badge/python-3.8%2B-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/backparq.svg)](https://pypi.org/project/backparq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## 🚀 Key Features

*   **Two Operation Modes**:
    *   **Offload (Archive) Mode**: Moves "cold" data (older than X days) to S3 and safely deletes it from the database to save space and improve performance.
    *   **Backup Mode**: Creates full, point-in-time snapshots of tables for disaster recovery without modifying source data.
*   **Performance**:
    *   **Parallel Processing**: Concurrent processing at both table and chunk levels.
    *   **Connection Pooling**: Efficient database connection management for high concurrency.
    *   **Streaming**: Uses server-side cursors to stream data, keeping memory usage constant regardless of table size.
*   **Safety First**:
    *   **Checksum Verification**: SHA256 checksums are computed and verified after every upload *before* any data is deleted from the source.
    *   **Graceful Shutdown**: Handles OS signals (SIGINT/SIGTERM) to stop cleanly without data corruption.
    *   **Atomic Operations**: Data is deleted in consistent batches only after successful verification.
*   **Restoration**:
    *   **Point-in-Time Restore**: Restore data for specific date ranges.
    *   **Conflict Resolution**: Choose between `do_nothing` (skip existing) or `upsert` (update existing) on restore.
    *   **Schema Evolution**: Automatically handles scenarios where the archive has columns that have been dropped from the live database.
*   **Observability**:
    *   **Rich CLI**: Progress bars, colored status updates, and statistics.
    *   **Structured Logging**: JSON logging support for integration with log aggregators (ELK, Datadog, etc.).
    *   **Verification**: Dedicated `check` and `verify` commands to audit S3 archives.

## 📦 Installation

```bash
pip install backparq
```

With optional features:

```bash
pip install backparq[all]      # All optional dependencies
pip install backparq[query]    # DuckDB for querying archives
pip install backparq[metrics]  # Prometheus metrics
```

## 🛠️ Quick Start

1.  **Generate a configuration file**:
    ```bash
    backparq init
    ```

2.  **Test connections** to Database and S3:
    ```bash
    backparq test --config backparq.yaml
    ```

3.  **Run an Archive** (Offload old data):
    ```bash
    # Dry run to see what would happen
    backparq archive --config backparq.yaml --dry-run
    
    # Execute with statistics
    backparq archive --config backparq.yaml --stats
    ```

## 📖 Usage & Commands

```text
usage: backparq [-h] [-v] {test,archive,restore,check,prune,status,verify,init} ...
```

### 1. Archiving (Offloading)
Moves data older than a cutoff date to S3 and deletes it from PostgreSQL.

```bash
backparq archive --config backparq_offload.yaml --stats
```
*   **Config tip**: Set `mode: offload` and `perform_delete: true`.

### 2. Backup
Takes a full snapshot of the table. Does not delete data.

```bash
backparq archive --config backparq_backup.yaml --stats
```
*   Creates a snapshot under a unique Run ID (timestamp).
*   **Config tip**: Set `mode: backup`.

### 3. Restore
Restores data from S3 back to PostgreSQL.

```bash
# Restore specific date range
backparq restore --config backparq.yaml --start 2024-01-01 --end 2024-02-01

# Restore from a specific backup snapshot
backparq restore --config backparq.yaml --backup-id 2024-01-15_120000 --start 2024-01-01 --end 2024-01-02

# Handle conflicts by updating existing rows
backparq restore --config backparq.yaml --start 2024-01-01 --end 2024-01-02 --conflict-mode upsert
```

### 4. Verification & Maintenance
Start `check` to quickly list archives, or `verify` for a deep content check.

```bash
# List archives in S3
backparq check --config backparq.yaml

# verify integrity of all archives (downloads and checks headers)
backparq verify --config backparq.yaml

# Delete old backups based on retention policy
backparq prune --config backparq.yaml
```

## ⚙️ Configuration

Configuration is YAML-based. You can use environment variables like `${VAR_NAME}` for sensitive values.

### Minimal Example
```yaml
database:
  host: localhost
  name: production_db
  user: postgres
  password: "${PG_PASSWORD}"

s3:
  bucket: my-company-backups
  prefix: app-data
  region: us-east-1

archive:
  mode: offload
  tables:
    - public.events
    - public.audit_logs
```

### Full Configuration Reference

See [backparq_full_example.yaml](examples/backparq_full_example.yaml) for a completely documented configuration file covering encryption, compression, retention policies, and advanced S3 settings.

## 🛡️ Security

*   **Identities**: Use IAM roles or environment variables. API keys are supported but recommended via env vars.
*   **Encryption**: Backparq supports S3 Server-Side Encryption (SSE-S3, SSE-KMS) and Client-Side Parquet encryption.
*   **Network**: Runs inside your infrastructure; no data is sent to Backparq servers.

## 🤝 Contributing

We welcome contributions!

```bash
# Install dev environment
pip install -e ".[dev]"

# Run tests
pytest

# Linting
ruff check src/
```

## License

MIT
