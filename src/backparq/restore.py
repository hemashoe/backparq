"""
Backparq Restore Module

Handles restoring archived data from S3/local Parquet files back to PostgreSQL.
Supports schema evolution (dropped columns) and conflict resolution.
"""

from __future__ import annotations

import datetime as dt
import logging
from typing import Optional

from backparq.archive import chunk_paths, s3_key_for_chunk
from backparq.config import BackparqConfig
from backparq.db import (
    ChunkSpec,
    add_months,
    connect_pg,
    insert_arrow_table_to_pg,
    month_floor,
    pg_get_columns,
)
from backparq.storage.parquet import read_parquet as read_chunk
from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.storage.s3 import verify_connection as test_s3_connection
from backparq.utils.console import console
from backparq.utils.logging import log_with_data

logger = logging.getLogger(__name__)


def restore_tables(
    config: BackparqConfig,
    start_date: dt.datetime,
    end_date: dt.datetime,
    dry_run: bool = False,
    conflict_mode: str = "do_nothing",
    backup_id: Optional[str] = None,
) -> None:
    """
    Restore archived data from S3/local back to PostgreSQL.

    Args:
        config: Backparq configuration
        start_date: Start of date range to restore (inclusive)
        end_date: End of date range to restore (exclusive)
        dry_run: If True, simulate restore without writing
        conflict_mode: "do_nothing" (skip existing) or "upsert" (override)
        backup_id: If restoring from backup mode, specify the run ID
    """
    log_with_data(
        logger,
        logging.INFO,
        "Starting restore",
        start=str(start_date.date()),
        end=str(end_date.date()),
        mode=conflict_mode,
        dry_run=dry_run,
        backup_id=backup_id,
    )

    conn = connect_pg(config.database)
    s3 = None

    if config.s3.bucket:
        try:
            test_s3_connection(config.s3)
            s3 = s3_client_from_config(config.s3)
            logger.info(f"S3 connection established: s3://{config.s3.bucket}")
        except Exception as e:
            logger.warning(f"S3 connection failed: {e}. Will use local files only.")

    try:
        for table_config in config.archive.tables:
            table = table_config.name
            primary_key = table_config.primary_key

            console.print(f"Restoring [bold]{table}[/bold]")
            if backup_id:
                console.print(f"Source: [cyan]{backup_id}[/cyan]")

            # Get current DB schema
            db_columns = set(pg_get_columns(conn, table))

            # Iterate through monthly chunks
            curr = month_floor(start_date)
            cutoff = end_date

            # Only scan months where restore is needed
            while curr < cutoff:
                next_month = add_months(curr, 1)
                chunk_spec = ChunkSpec(table=table, start=curr, end=next_month)

                # Get local file paths
                final_parquet, _, _, manifest_path = chunk_paths(
                    config.archive.base_dir, chunk_spec
                )

                # Download from S3 if needed
                files_to_restore = []

                if backup_id:
                    # Backup mode: list files from the backup directory
                    year = curr.year
                    month = curr.month
                    safe_table = table.replace(".", "_")
                    # Backups use a stricter structure with run_id prefix
                    s3_prefix_dir = (
                        f"{config.s3.prefix}/backups/{backup_id}/"
                        f"{safe_table}/year={year:04d}/month={month:02d}/"
                    )
                    need_download = True
                else:
                    # Offload mode: Directory might contain multiple files (incremental runs)
                    year = curr.year
                    month = curr.month
                    safe_table = table.replace(".", "_")
                    s3_prefix_dir = f"{config.s3.prefix}/archive/{safe_table}/year={year:04d}/month={month:02d}/"
                    # Check local first? If purely local restore?
                    # But we want to sync from S3.
                    # If we don't have S3 config, we rely on local files.
                    if s3:
                        need_download = True
                    else:
                        # Local only: find files in directory
                        chunk_dir = (
                            config.archive.base_dir
                            / "parquet"
                            / safe_table
                            / f"year={year:04d}"
                            / f"month={month:02d}"
                        )
                        if chunk_dir.exists():
                            files_to_restore = list(chunk_dir.glob("*.parquet"))
                        need_download = False

                if need_download and s3:
                    if not dry_run:
                        try:
                            # List objects in the directory
                            response = s3.list_objects_v2(
                                Bucket=config.s3.bucket, Prefix=s3_prefix_dir
                            )
                            if "Contents" in response:
                                for obj in response["Contents"]:
                                    key = obj["Key"]
                                    if not key.endswith(".parquet"):
                                        continue

                                    filename = key.split("/")[-1]
                                    chunk_dir = (
                                        config.archive.base_dir
                                        / "parquet"
                                        / safe_table
                                        / f"year={year:04d}"
                                        / f"month={month:02d}"
                                    )
                                    chunk_dir.mkdir(parents=True, exist_ok=True)

                                    local_file = chunk_dir / filename
                                    files_to_restore.append(local_file)

                                    # Download if not exists or verify?
                                    # For restore, we usually download.
                                    s3.download_file(config.s3.bucket, key, local_file.as_posix())
                                    log_with_data(
                                        logger, logging.DEBUG, "Downloaded chunk", key=key
                                    )
                            else:
                                # No files in S3 for this month
                                pass
                        except Exception as e:
                            logger.error(
                                f"Failed to list/download S3 objects for {s3_prefix_dir}: {e}"
                            )
                            curr = next_month
                            continue
                    else:
                        logger.info(f"DRY RUN: would list and restore files from {s3_prefix_dir}")
                        curr = next_month  # Skip rest of loop for dry run
                        continue

                # If we are in local-only mode (need_download=False), files_to_restore is already set

                if not files_to_restore:
                    # No files found (S3 or Local)
                    curr = next_month
                    continue

                for final_parquet in files_to_restore:
                    try:
                        table_arrow = read_chunk(final_parquet)
                        parquet_cols = set(table_arrow.column_names)
                        common_cols = list(parquet_cols.intersection(db_columns))

                        if not common_cols:
                            logger.warning(f"No common columns for {chunk_spec}")
                            curr = next_month
                            continue

                        # Filter and Insert
                        table_arrow_filtered = table_arrow.select(common_cols)
                        inserted = insert_arrow_table_to_pg(
                            conn=conn,
                            table=table,
                            arrow_table=table_arrow_filtered,
                            conflict_mode=conflict_mode,
                            primary_key=primary_key,
                        )

                        log_with_data(
                            logger,
                            logging.INFO,
                            "Restored chunk",
                            table=table,
                            month=curr.strftime("%Y-%m"),
                            rows=inserted,
                        )

                    except Exception as e:
                        logger.error(f"Failed to restore chunk {chunk_spec}: {e}")

                curr = next_month

    finally:
        conn.close()
        logger.info("Restore complete")
