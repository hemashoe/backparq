# Backparq: Postgres to S3 Parquet Archiver

**Backparq** is a robust data lifecycle tool that bridges the gap between your transactional database (Postgres) and your data lake (S3). It safely moves old data from active tables into compressed, queryable Parquet files on S3, and allows you to restore strictly defined chunks when needed.

It is designed for high-volume production environments where deleting old data ("Pruning") is necessary for performance, but keeping that data accessible for auditing or analytics is required.

## Key Features

-   **Archive & Offload**: Move cold data (e.g., older than 3 months) to S3 Parquet.
-   **Safety**: Checksums (SHA256) are verified after upload and *before* any data is deleted from DB.
-   **Restore**: Seamlessly copy data back from S3 to Postgres. Handles schema evolution (column drops) and complex types (JSON/Arrays) automatically.
-   **Maintenance**: Built-in `prune` command to manage retention of S3 backups.
-   **Performance**: Supports parallel processing of tables and *intra-table* chunks to maximize throughput.
-   **Observability**: CLI commands to `check` backup status and progress bars for long-running ops.

## Installation

```bash
# Standard installation
pip install backparq

# With dev dependencies (for testing)
pip install "backparq[dev]"
```

## Configuration

Backparq is purely config-driven. Create a `config.yaml` file:

```yaml
database:
  dsn: "postgresql://user:pass@localhost:5432/mydb"

s3:
  bucket: "my-company-data-lake"
  prefix: "app_data"
  region: "us-east-1"

archive:
  tables: ["public.events", "public.audit_logs"]
  
  # MODE: "offload" (Default) OR "backup"
  # - offload: Moves OLD data to S3 (archive/...) and optionally deletes from DB.
  #            Retention checks DATA AGE.
  # - backup:  Creates FULL snapshot (backups/...) for Disaster Recovery.
  #            Retention checks SNAPSHOT AGE.
  mode: "offload" 
  
  concurrency: 2
  chunk_concurrency: 4
  
  # [Offload Mode Only] Archive everything older than this date.
  cutoff_exclusive: "2024-01-01T00:00:00Z"
  
  # [Offload Mode Only] DELETES rows from Postgres after upload.
  perform_delete: false

  # Retention Policy
  # - In "offload" mode: Delete data older than 12 months.
  # - In "backup" mode: Delete snapshots created older than 30 days.
  retention:
    enabled: true
    months: 12
    days: 30
```

See [config.example.yaml](config.example.yaml) for all options (including Encryption).

## Usage

### 1. Backup (Archive)
Runs the archive process. Reads PG, converts to Parquet, uploads to S3.
If `perform_delete: true` in config, it will also delete the source rows.

```bash
backparq archive --config config.yaml
```

### 2. Check Backups
List what is currently stored in S3, aggregated by table and month.

```bash
backparq check --config config.yaml
```

### 3. Restore (Rollback)
Restore a specific date range of data back into Postgres.
Supports `do_nothing` (skip existing) or `upsert` (override).

```bash
backparq restore \
  --config config.yaml \
  --start 2023-01-01 \
  --end 2023-03-01 \
  --conflict-mode do_nothing
```

### 4. Prune Retention
Delete old S3 backups based on your `retention` policy in config.

```bash
# Dry run to see what would be deleted
backparq prune --config config.yaml --dry-run

# Execute delete
backparq prune --config config.yaml
```


## How It Works

1.  **Chunks**: Data is partitioned by Month (`YYYY-MM`).
2.  **Streaming**: Data is streamed using server-side cursors to effectively use constant RAM regardless of table size.
3.  **Atomic**: A Manifest file (`manifest.json`) is written to local disk (and S3) only after the Parquet file is successfully written and verified.
4.  **Schema Evolution**: When restoring, Backparq fetches the *current* DB schema. If the Parquet file has columns that no longer exist in the DB, they are ignored.

## License
MIT
