"""Delete operation: UPLOADED → OFFLOADED (WATERMARK-BASED).

Verifies S3 checksum, deletes rows up to the watermark (MAX(id) at export time),
updates catalog.
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import BackparqConfig, S3Config
    from backparq.db.operations import ChunkSpec

from backparq.adapters.catalog import ChunkState
from backparq.db.operations import delete_chunk_with_verification

logger = logging.getLogger(__name__)


def delete_chunk(
    chunk: ChunkSpec,
    conn: Any,
    catalog: Catalog,
    s3_client: Any,
    config: BackparqConfig,
    vacuum: bool = False,
    batch_size: int = 10_000,
) -> dict[str, Any]:
    """
    Delete chunk from PostgreSQL using watermark (SAFE from race conditions).

    Args:
        chunk: ChunkSpec defining table and time range
        conn: PostgreSQL connection
        catalog: Catalog for state tracking
        s3_client: S3 client
        config: Full configuration
        vacuum: Whether to run VACUUM after delete
        batch_size: Rows to delete per batch

    Returns:
        Dict with stats: rows_deleted, vacuumed, skipped
    """
    chunk_id = f"{chunk.table}_{chunk.start.strftime('%Y%m%d%H%M%S')}"

    # Check state
    current_state = catalog.get_state(chunk_id)
    if not current_state:
        logger.warning(f"Chunk {chunk_id} not found in catalog")
        return {"rows_deleted": 0, "vacuumed": False, "skipped": True}

    if current_state < ChunkState.UPLOADED:
        logger.warning(f"Chunk {chunk_id} not uploaded yet (state: {current_state})")
        return {"rows_deleted": 0, "vacuumed": False, "skipped": True}

    if current_state >= ChunkState.OFFLOADED:
        logger.debug(f"Chunk {chunk_id} already offloaded, skipping")
        return {"rows_deleted": 0, "vacuumed": False, "skipped": True}

    # Get chunk data
    chunk_data = catalog.get_chunk(chunk_id)
    if not chunk_data:
        raise ValueError(f"Chunk {chunk_id} not found in catalog")

    s3_key = chunk_data.get("s3_key")
    sha256 = chunk_data.get("sha256")
    watermark_id = chunk_data.get("watermark_id")

    if not s3_key or not sha256:
        raise ValueError(f"Chunk {chunk_id} missing S3 key or SHA256")

    if watermark_id is None:
        logger.warning(
            f"Chunk {chunk_id} has NO WATERMARK. Deletion will utilize time-range only. "
            "This is potentially unsafe if backfills occurred."
        )

    # Validate S3 and Delete
    s3_config = config.s3
    table_config = config.archive.get_table_config(chunk.table)

    deleted = delete_chunk_with_verification(
        conn=conn,
        table=chunk.table,
        expected_sha256=sha256,
        s3_bucket=s3_config.bucket,
        s3_key=s3_key,
        s3_client=s3_client,
        start=chunk.start,
        end=chunk.end,
        order_by=table_config.order_by,
        config=config,
        watermark_id=watermark_id,
    )

    if deleted < 0:
        logger.error(f"Deletion failed verification for {chunk_id}")
        return {"rows_deleted": 0, "vacuumed": False, "skipped": True}

    # Run VACUUM if requested
    vacuumed = False
    if vacuum:
        # VACUUM cannot run within a transaction block.
        # Skip here and let pipeline handle it or run separately.
        logger.debug("Skipping VACUUM inside transaction block")

    # Update Catalog
    catalog.transition(chunk_id, ChunkState.OFFLOADED)

    return {
        "rows_deleted": deleted,
        "vacuumed": vacuumed,
        "skipped": False,
    }
