# backup-parquet-s3

Archive Postgres tables to local Parquet files, upload to S3, and optionally delete archived rows. The package exposes a CLI and a Prefect flow for orchestration.

## Installation

```bash
pip install -e .
```

With Prefect:

```bash
pip install -e ".[prefect]"
```

## Environment variables

- DB: `PG_DSN` (preferred) OR `PG_HOST`/`PG_PORT`/`PG_DBNAME`/`PG_USER`/`PG_PASSWORD` (+ optional `PG_SSLMODE`)
- S3: `S3_BUCKET`, `S3_PREFIX`
- AWS: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` (+ optional `AWS_SESSION_TOKEN`)
- Optional S3 SSE: `S3_SSE` (AES256 or aws:kms) and `S3_KMS_KEY_ID`

## CLI usage

```bash
archive-to-parquet \
  --tables public.billing,public.events \
  --cutoff-exclusive 2025-08-01 \
  --perform-delete
```

## Prefect

```python
from backup_parquet_s3.flows import archive_flow

archive_flow(
    tables=["public.billing"],
    cutoff_exclusive="2025-08-01",
)
```
