from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import S3Config
    from backparq.db.operations import ChunkSpec

logger = logging.getLogger(__name__)


def restore_chunk(
    chunk: ChunkSpec,
    conn: Any,
    catalog: Catalog,
    s3_client: Any,
    s3_config: S3Config,
    conflict_mode: str = "do_nothing",
    local_cache_dir: Path | None = None,
) -> dict[str, Any]:
    """
    Restore chunk from S3 to PostgreSQL.

    Args:
        chunk: ChunkSpec defining table and time range
        conn: PostgreSQL connection
        catalog: Catalog for state tracking
        s3_client: boto3 S3 client
        s3_config: S3 configuration
        conflict_mode: "do_nothing" or "update" for conflict resolution
        local_cache_dir: Optional local cache directory

    Returns:
        Dict with stats: rows_restored

    Raises:
        Exception: If restore fails
    """
    import tempfile

    import pyarrow.parquet as pq

    from backparq.db.operations import insert_arrow_table_to_pg
    from backparq.primitives.checksum import compute_sha256
    from backparq.storage.s3 import download_file

    chunk_id = f"{chunk.table}_{chunk.start.strftime('%Y%m%d%H%M%S')}"

    # Get chunk data from catalog
    chunk_data = catalog.get_chunk(chunk_id)
    if not chunk_data:
        raise ValueError(f"Chunk {chunk_id} not found in catalog")

    s3_key = chunk_data.get("s3_key")
    if not s3_key:
        raise ValueError(f"Chunk {chunk_id} has no S3 key")

    try:
        # Download from S3 to temp file
        if local_cache_dir:
            local_cache_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_cache_dir / Path(s3_key).name
            temp_dir = None
        else:
            temp_dir = tempfile.mkdtemp()
            local_path = Path(temp_dir) / Path(s3_key).name

        logger.info(f"Downloading {chunk_id} from s3://{s3_config.bucket}/{s3_key}")
        download_file(s3_client, s3_config.bucket, s3_key, str(local_path))

        # Verify SHA256 after download
        expected_sha256 = chunk_data.get("sha256")
        if expected_sha256:
            actual_sha256 = compute_sha256(local_path)
            if actual_sha256 != expected_sha256:
                raise ValueError(
                    f"SHA256 mismatch for {chunk_id}: "
                    f"expected {expected_sha256}, got {actual_sha256}"
                )
            logger.debug(f"SHA256 verified for {chunk_id}")

        # Read Parquet
        table = pq.read_table(local_path)

        # Insert into PostgreSQL
        logger.info(f"Restoring {chunk_id} to PostgreSQL ({table.num_rows} rows)")
        insert_arrow_table_to_pg(
            conn=conn, table=chunk.table, arrow_table=table, conflict_mode=conflict_mode
        )

        logger.info(f"Restored {chunk_id}: {table.num_rows} rows")

        return {"rows_restored": table.num_rows, "skipped": False}

    except Exception as e:
        logger.error(f"Restore failed for {chunk_id}: {e}")
        raise
    finally:
        # Clean up temp directory if we created one
        if temp_dir is not None:
            import shutil

            shutil.rmtree(temp_dir, ignore_errors=True)
