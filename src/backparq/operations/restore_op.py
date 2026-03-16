from __future__ import annotations

import logging
import shutil
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pyarrow.parquet as pq

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import S3Config
    from backparq.db.operations import ChunkSpec

from backparq.db.operations import insert_arrow_table_to_pg
from backparq.primitives import chunk_id as make_chunk_id
from backparq.primitives.checksum import compute_sha256
from backparq.storage.s3 import download_file

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
        conflict_mode: "do_nothing" or "upsert" for conflict resolution
        local_cache_dir: Optional local cache directory

    Returns:
        Dict with stats: rows_restored (actual rows inserted, not file row count)

    Raises:
        ValueError: If chunk not found, S3 key missing, or SHA256 mismatch
    """
    cid = make_chunk_id(chunk)

    # Get chunk data from catalog
    chunk_data = catalog.get_chunk(cid)
    if not chunk_data:
        raise ValueError(f"Chunk {cid} not found in catalog")

    s3_key = chunk_data.get("s3_key")
    if not s3_key:
        raise ValueError(f"Chunk {cid} has no S3 key")

    temp_dir: str | None = None
    try:
        # Download from S3 to temp file
        if local_cache_dir:
            local_cache_dir.mkdir(parents=True, exist_ok=True)
            local_path = local_cache_dir / Path(s3_key).name
        else:
            temp_dir = tempfile.mkdtemp()
            local_path = Path(temp_dir) / Path(s3_key).name

        logger.info(f"Downloading {cid} from s3://{s3_config.bucket}/{s3_key}")
        download_file(s3_client, s3_config.bucket, s3_key, str(local_path))

        # Verify SHA256 after download
        expected_sha256 = chunk_data.get("sha256")
        if expected_sha256:
            actual_sha256 = compute_sha256(local_path)
            if actual_sha256 != expected_sha256:
                raise ValueError(
                    f"SHA256 mismatch for {cid}: "
                    f"expected {expected_sha256}, got {actual_sha256}"
                )
            logger.debug(f"SHA256 verified for {cid}")
        else:
            logger.warning(f"No SHA256 in catalog for {cid}; skipping integrity check")

        # Read Parquet and insert — report actual inserted count, not file row count.
        # insert_arrow_table_to_pg respects conflict_mode and returns affected rows.
        arrow_table = pq.read_table(local_path)

        logger.info(f"Restoring {cid} to PostgreSQL ({arrow_table.num_rows} rows in file)")
        rows_inserted = insert_arrow_table_to_pg(
            conn=conn,
            table=chunk.table,
            arrow_table=arrow_table,
            conflict_mode=conflict_mode,
        )

        logger.info(f"Restored {cid}: {rows_inserted} rows inserted")
        return {"rows_restored": rows_inserted, "skipped": False}

    except Exception as e:
        logger.error(f"Restore failed for {cid}: {e}")
        raise
    finally:
        if temp_dir is not None:
            shutil.rmtree(temp_dir, ignore_errors=True)
