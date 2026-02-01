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
from backparq.utils.console import console
from backparq.db import (
    ChunkSpec,
    add_months,
    connect_pg,
    insert_arrow_table_to_pg,
    month_floor,
    pg_get_columns,
)
from backparq.storage.parquet import read_parquet as read_chunk
from backparq.storage.s3 import create_client as s3_client_from_config, verify_connection as test_s3_connection

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
    log_with_data(logger, logging.INFO, "Starting restore", 
                  start=str(start_date.date()), end=str(end_date.date()), 
                  mode=conflict_mode, dry_run=dry_run, backup_id=backup_id)

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

                # Determine if we need to download from S3
                need_download = False
                if backup_id:
                    need_download = True
                elif not final_parquet.exists():
                    need_download = True

                # Download from S3 if needed
                if need_download:
                    if s3:
                        if backup_id:
                            year = curr.year
                            month = curr.month
                            safe_table = table.replace(".", "_")
                            name = f"{safe_table}_{year:04d}-{month:02d}.parquet"
                            s3_key = (
                                f"{config.s3.prefix}/backups/{backup_id}/"
                                f"{safe_table}/year={year:04d}/month={month:02d}/{name}"
                            )
                        else:
                            s3_key = s3_key_for_chunk(config.s3.prefix, chunk_spec, mode="offload")

                        if not dry_run:
                            try:
                                final_parquet.parent.mkdir(parents=True, exist_ok=True)
                                s3.download_file(config.s3.bucket, s3_key, final_parquet.as_posix())

                                # Verify checksum
                                try:
                                    head = s3.head_object(Bucket=config.s3.bucket, Key=s3_key)
                                    expected_sha = head.get("Metadata", {}).get("sha256", "")
                                    if expected_sha:
                                        from backparq.parquet import sha256_file
                                        actual_sha = sha256_file(final_parquet)
                                        if actual_sha != expected_sha:
                                            logger.error(f"Checksum mismatch for {s3_key}")
                                            final_parquet.unlink(missing_ok=True)
                                            curr = next_month
                                            continue
                                except Exception:
                                    pass

                                log_with_data(logger, logging.DEBUG, "Downloaded chunk", key=s3_key)
                            except Exception:
                                # Siltently skip missing chunks in S3 (expected for empty months)
                                curr = next_month
                                continue
                        else:
                            logger.info(f"DRY RUN: would download {s3_key}")
                    else:
                        if not final_parquet.exists():
                            curr = next_month
                            continue

                # Restore data from parquet file
                if dry_run:
                    logger.info(f"DRY RUN: would restore {final_parquet}")
                else:
                    if not final_parquet.exists():
                        curr = next_month
                        continue

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
                        
                        log_with_data(logger, logging.INFO, "Restored chunk", 
                                      table=table, month=curr.strftime("%Y-%m"), rows=inserted)

                    except Exception as e:
                        logger.error(f"Failed to restore chunk {chunk_spec}: {e}")

                curr = next_month

    finally:
        conn.close()
        logger.info("Restore complete")
