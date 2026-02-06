"""Time-based chunking and path generation - pure functions."""

from __future__ import annotations

import datetime as dt
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from backparq.db.operations import ChunkSpec


def normalize_dt(value: dt.datetime) -> dt.datetime:
    """Normalize a datetime to UTC timezone-aware."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def month_floor(value: dt.datetime) -> dt.datetime:
    """Return the first day of the month for the given datetime."""
    value = normalize_dt(value)
    return dt.datetime(value.year, value.month, 1, tzinfo=dt.timezone.utc)


def add_months(value: dt.datetime, months: int) -> dt.datetime:
    """Add months to a datetime, returning the first of the resulting month."""
    value = normalize_dt(value)
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    return dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)


def get_chunk_filename(chunk: ChunkSpec, suffix: str = "") -> str:
    """
    Generate unique filename for a chunk.

    Args:
        chunk: ChunkSpec with table, start, end
        suffix: Optional suffix to add before .parquet extension

    Returns:
        Filename string (e.g., "public_events_2024-03_20240301120000_abc123.parquet")
    """
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")
    start_str = chunk.start.strftime("%Y%m%d%H%M%S")
    return f"{safe_table}_{year:04d}-{month:02d}_{start_str}{suffix}.parquet"


def chunk_paths(base_dir: Path, chunk: ChunkSpec) -> tuple[Path, Path, Path, Path]:
    """
    Generate file paths for a chunk.

    Args:
        base_dir: Base directory for Parquet files
        chunk: ChunkSpec with table, start, end

    Returns:
        Tuple of (final_path, inprogress_path, sha256_path, manifest_path)
    """
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")

    # Use random suffix for local temp file to avoid collision if parallel
    name = get_chunk_filename(chunk, suffix=f"_{uuid.uuid4().hex[:8]}")

    chunk_dir = base_dir / "parquet" / safe_table / f"year={year:04d}" / f"month={month:02d}"

    final = chunk_dir / name
    inprogress = chunk_dir / f"{name}.inprogress"
    sha_path = chunk_dir / f"{name}.sha256"
    manifest = chunk_dir / f"{name}.manifest.json"

    return final, inprogress, sha_path, manifest


def s3_key_for_chunk(
    base_prefix: str, chunk: ChunkSpec, mode: str, run_id: str | None = None
) -> str:
    """
    Generate S3 key for a chunk.

    Args:
        base_prefix: S3 prefix (e.g., "db-archive")
        chunk: ChunkSpec with table, start, end
        mode: "backup" or "offload"
        run_id: Required for backup mode

    Returns:
        S3 key string

    Raises:
        ValueError: If mode is "backup" and run_id is not provided
    """
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")

    # For S3, we want stable but unique names. run_id helps if provided.
    suffix = f"_{run_id}" if run_id else ""
    name = get_chunk_filename(chunk, suffix=suffix)

    if mode == "backup":
        if not run_id:
            raise ValueError("run_id required for backup mode")
        return (
            f"{base_prefix}/backups/{run_id}/{safe_table}/year={year:04d}/month={month:02d}/{name}"
        )
    return f"{base_prefix}/archive/{safe_table}/year={year:04d}/month={month:02d}/{name}"
