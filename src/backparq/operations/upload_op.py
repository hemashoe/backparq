"""Upload operation: EXPORTED → UPLOADED.

Uploads Parquet file to S3, verifies checksum, updates catalog.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import S3Config
    from backparq.db.operations import ChunkSpec

from backparq.adapters.catalog import ChunkState
from backparq.primitives import s3_key_for_chunk

logger = logging.getLogger(__name__)


def upload_chunk(
    chunk: ChunkSpec,
    catalog: Catalog,
    s3_client: Any,
    s3_config: S3Config,
    mode: str,
    run_id: str | None = None,
    extra_args: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """
    Upload chunk to S3 and verify checksum.

    Args:
        chunk: ChunkSpec defining table and time range
        catalog: Catalog for state tracking
        s3_client: boto3 S3 client
        s3_config: S3 configuration
        mode: "backup" or "offload"
        run_id: Run ID (required for backup mode)
        extra_args: Extra S3 upload args (SSE, etc.)

    Returns:
        Dict with stats: s3_key, verified

    Raises:
        Exception: If upload or verification fails
    """
    from backparq.storage.s3 import upload_file, verify_checksum

    chunk_id = f"{chunk.table}_{chunk.start.strftime('%Y%m%d%H%M%S')}"

    # Check current state
    current_state = catalog.get_state(chunk_id)
    if not current_state:
        raise ValueError(f"Chunk {chunk_id} not found in catalog")

    if current_state >= ChunkState.UPLOADED:
        logger.debug(f"Chunk {chunk_id} already uploaded, skipping")
        chunk_data = catalog.get_chunk(chunk_id)
        return {
            "s3_key": chunk_data.get("s3_key", "") if chunk_data else "",
            "verified": True,
            "skipped": True,
        }

    if current_state < ChunkState.EXPORTED:
        raise ValueError(f"Chunk {chunk_id} not exported yet (state={current_state.value})")

    # Get chunk data
    chunk_data = catalog.get_chunk(chunk_id)
    if not chunk_data:
        raise ValueError(f"Chunk {chunk_id} data not found")

    local_path = Path(chunk_data["local_path"])
    if not local_path.exists():
        raise FileNotFoundError(f"Local file not found: {local_path}")

    sha256 = chunk_data["sha256"]

    # Generate S3 key
    s3_key = s3_key_for_chunk(s3_config.prefix, chunk, mode, run_id)

    try:
        logger.info(f"Uploading {chunk_id} to s3://{s3_config.bucket}/{s3_key}")

        # Upload with metadata
        metadata = {
            "table": chunk.table,
            "start": chunk.start.isoformat(),
            "end": chunk.end.isoformat(),
            "rows": str(chunk_data["row_count"]),
            "sha256": sha256,
        }

        upload_file(
            s3=s3_client,
            path=local_path,
            bucket=s3_config.bucket,
            key=s3_key,
            sha256=sha256,
            extra_args=extra_args,
            metadata=metadata,
        )

        # Verify checksum on S3
        verified = verify_checksum(s3_client, s3_config.bucket, s3_key, sha256)
        if not verified:
            raise ValueError(f"S3 checksum verification failed for {s3_key}")

        # Update catalog
        catalog.transition(chunk_id, ChunkState.UPLOADED, s3_key=s3_key)

        logger.info(f"Uploaded and verified {chunk_id}")

        return {"s3_key": s3_key, "verified": True, "skipped": False}

    except Exception as e:
        logger.error(f"Upload failed for {chunk_id}: {e}")
        raise
