# Backparq

Archive PostgreSQL tables to Parquet files on S3 with safety, restore, and retention management.

[![Python 3.9+](https://img.shields.io/badge/python-3.9%2B-blue.svg)](https://www.python.org/)
[![PyPI](https://img.shields.io/pypi/v/backparq.svg)](https://pypi.org/project/backparq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

## Features

- **Archive & Offload** - Move cold data to S3 Parquet, optionally delete from DB
- **Backup Mode** - Full table snapshots for disaster recovery
- **Safety First** - SHA256 checksums verified before any data deletion
- **Schema Evolution** - Restore handles dropped columns automatically
- **Parallel Processing** - Table-level and chunk-level concurrency
- **Graceful Shutdown** - Clean interruption on SIGINT/SIGTERM

## Installation

```bash
pip install backparq
```

With optional features:

```bash
pip install backparq[all]      # All optional dependencies
pip install backparq[query]    # DuckDB for querying archives
pip install backparq[metrics]  # Prometheus metrics
```

## Quick Start

```bash
# Generate config interactively
backparq init

# Test connections
backparq test --config backparq.yaml

# Run archive (dry-run first)
backparq archive --config backparq.yaml --dry-run -v

# Run archive
backparq archive --config backparq.yaml -v --stats
```

## Commands

```text
usage: backparq [-h] [-v] {test,archive,apply,restore,check,prune,status,verify,init} ...

Commands:
  test      Test connections
  archive   Archive tables to Parquet/S3
  apply     Archive and install cron
  restore   Restore from archive
  check     List S3 backups
  prune     Delete old backups
  status    Show archive status
  verify    Verify archive integrity
  init      Generate config file

Options:
  -v, --verbose   Verbosity (-v INFO, -vv DEBUG)
```

### archive

```bash
backparq archive --config config.yaml --stats
backparq archive --config config.yaml --output json
```

### restore

```bash
backparq restore --config config.yaml --start 2024-01-01 --end 2024-04-01
backparq restore --config config.yaml --start 2024-01-01 --end 2024-04-01 --conflict-mode upsert
```

### status

```bash
backparq status --config config.yaml
backparq status --config config.yaml --table events --output json
```

### verify

```bash
backparq verify --config config.yaml
backparq verify --config config.yaml --repair
```

## Configuration

### Basic

```yaml
database:
  host: localhost
  port: 5432
  name: mydb
  user: postgres
  password: "${PG_PASSWORD}"

s3:
  bucket: my-backup-bucket
  prefix: db-archive
  region: us-east-1

archive:
  mode: offload
  tables:
    - public.events
    - public.orders
```

### Table Primary Keys

```yaml
archive:
  tables:
    - public.events                    # Uses default "id"
    - table: public.orders
      primary_key: order_id            # Custom primary key
```

The `primary_key` is used during `restore --conflict-mode upsert` to detect and update existing rows.

### Full Example

```yaml
database:
  host: localhost
  port: 5432
  name: production
  user: backup_user
  password: "${PG_PASSWORD}"
  sslmode: require

s3:
  bucket: company-backups
  prefix: postgres/archive
  region: us-east-1
  access_key_id: "${AWS_ACCESS_KEY_ID}"
  secret_access_key: "${AWS_SECRET_ACCESS_KEY}"
  sse: aws:kms
  kms_key_id: alias/backup-key

archive:
  mode: offload
  order_by: created_at
  cutoff: -90d
  perform_delete: false
  concurrency: 2
  base_dir: ./backparq-data

  tables:
    - public.events
    - table: public.orders
      primary_key: order_id

  retention:
    enabled: true
    days: 365

parquet:
  compression: zstd
  row_group_size: 100000
```

## Archive Modes

### Offload Mode (Default)

Archives data older than `cutoff` date, partitioned by month.

```yaml
archive:
  mode: offload
  order_by: created_at
  cutoff: -90d
  perform_delete: true
```

### Backup Mode

Creates full table snapshots with unique run ID.

```yaml
archive:
  mode: backup
```

Restore from snapshot:

```bash
backparq restore --config config.yaml --backup-id 2024-01-15_120000 --start 2024-01-01 --end 2024-02-01
```

## Testing

### Local with MinIO

```bash
# Start MinIO
docker run -d -p 9000:9000 -p 9001:9001 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio server /data --console-address ":9001"

# Create config
cat > config.yaml << 'EOF'
database:
  host: localhost
  port: 5432
  name: testdb
  user: postgres
  password: postgres

s3:
  bucket: test-bucket
  prefix: backparq
  endpoint_url: http://localhost:9000
  access_key_id: minioadmin
  secret_access_key: minioadmin
  addressing_style: path

archive:
  mode: offload
  tables:
    - public.test_table
EOF

# Test
backparq test --config config.yaml
backparq archive --config config.yaml -v --stats
```

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest

# Type check
mypy src/backparq

# Lint
ruff check src/
```

## License

MIT
