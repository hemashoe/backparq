"""Export operation: IN_DB → EXPORTED (SNAPSHOT + WATERMARK).

Uses PostgreSQL 'REPEATABLE READ' isolation with specific snapshot ID
to ensure consistent view across all chunks.
Uses 'watermark_id' (MAX(id)) to define a safe cutoff point for deletion.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import DatabaseConfig, ParquetConfig, TableConfig
    from backparq.db.operations import ChunkSpec

from backparq.adapters.catalog import ChunkState
from backparq.db.operations import export_chunk_to_parquet_streaming
from backparq.primitives import chunk_paths, compute_sha256

logger = logging.getLogger(__name__)


def export_chunk(
    chunk: ChunkSpec,
    conn: Any,
    catalog: Catalog,
    base_dir: Path,
    parquet_config: ParquetConfig,
    table_config: TableConfig,
    snapshot_id: Optional[str] = None,
    watermark_id: Any = None,
    db_config: Optional[DatabaseConfig] = None,
) -> dict[str, Any]:
    """
    Export chunk from PostgreSQL to local Parquet file.

    Args:
        chunk: ChunkSpec defining table and time range
        conn: PostgreSQL connection (should be integrated with snapshot if snapshot_id provided)
        catalog: Catalog for state tracking
        base_dir: Base directory for Parquet files
        parquet_config: Parquet format configuration
        table_config: Table-specific configuration
        snapshot_id: Postgres Snapshot ID for consistency
        watermark_id: Max ID at the start of transaction (safe deletion cutoff)
        db_config: DatabaseConfig for building safe DuckDB DSN

    Returns:
        Dict with stats: rows_exported, bytes_written, sha256
    """
    chunk_id = f"{chunk.table}_{chunk.start.strftime('%Y%m%d%H%M%S')}"

    # Check state
    current_state = catalog.get_state(chunk_id)
    if current_state and current_state >= ChunkState.EXPORTED:
        logger.debug(f"Chunk {chunk_id} already exported, skipping")
        chunk_data = catalog.get_chunk(chunk_id)
        return {
            "rows_exported": chunk_data.get("row_count", 0) if chunk_data else 0,  # type: ignore
            "bytes_written": chunk_data.get("byte_size", 0) if chunk_data else 0,  # type: ignore
            "sha256": chunk_data.get("sha256", "") if chunk_data else "",  # type: ignore
            "skipped": True,
        }

    # Generate paths
    final_path, inprogress_path, sha_path, manifest_path = chunk_paths(base_dir, chunk)
    final_path.parent.mkdir(parents=True, exist_ok=True)

    try:
        logger.info(
            f"Exporting {chunk_id}: {chunk.start} to {chunk.end} (Snap: {snapshot_id}, WM: {watermark_id})"
        )

        # Build encryption properties
        encryption_properties = None
        if parquet_config.encryption and parquet_config.encryption.enabled:
            from backparq.storage.parquet import build_encryption

            encryption_properties = build_encryption(parquet_config.encryption)

        # Export using DuckDB
        rows_exported = export_chunk_to_parquet_streaming(
            conn=conn,
            table=chunk.table,
            start=chunk.start,
            end=chunk.end,
            parquet_path=inprogress_path,
            order_by=table_config.order_by,
            row_group_size=parquet_config.row_group_size,
            compression=parquet_config.compression,
            encryption_properties=encryption_properties,
            masking=table_config.masking,
            watermark_id=watermark_id,
            primary_key=table_config.primary_key,
            db_config=db_config,
        )

        if rows_exported == 0:
            logger.warning(f"No rows exported for {chunk_id}")
            # We still mark it as exported so we don't loop forever?
            # Or maybe we skip creating files?
            # If 0 rows, we might still want to record it so we can "delete" 0 rows later?
            # Creating empty parquet is fine.
            pass

        # Rename to final path
        inprogress_path.rename(final_path)

        # Compute SHA256
        sha256 = compute_sha256(final_path)
        byte_size = final_path.stat().st_size

        # Write SHA256 file
        sha_path.write_text(sha256)

        # Write manifest
        manifest = {
            "chunk_id": chunk_id,
            "table": chunk.table,
            "start": chunk.start.isoformat(),
            "end": chunk.end.isoformat(),
            "exported_rows": rows_exported,
            "sha256": sha256,
            "byte_size": byte_size,
            "snapshot_id": snapshot_id,
            "watermark_id": watermark_id,
        }
        manifest_path.write_text(json.dumps(manifest, indent=2))

        # Update Catalog
        logger.info(f"Updating catalog for {chunk_id}")
        catalog.transition(
            chunk_id,
            ChunkState.EXPORTED,
            table_name=chunk.table,
            start_ts=chunk.start.isoformat(),
            end_ts=chunk.end.isoformat(),
            local_path=str(final_path),
            sha256=sha256,
            row_count=rows_exported,
            byte_size=byte_size,
            watermark_id=watermark_id,  # Store watermark for deletion!
        )

        return {
            "rows_exported": rows_exported,
            "bytes_written": byte_size,
            "sha256": sha256,
            "skipped": False,
        }

    except Exception as e:
        if inprogress_path.exists():
            inprogress_path.unlink()
        logger.error(f"Export failed for {chunk_id}: {e}")
        raise
