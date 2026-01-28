"""
Backparq Restore Module

Handles restoring archived data from S3/local Parquet files back to PostgreSQL.
Supports schema evolution (dropped columns) and conflict resolution.
"""

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
from backparq.parquet import read_chunk
from backparq.s3 import s3_client_from_config, test_s3_connection

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
    logger.info(
        f"Starting restore: {start_date.date()} to {end_date.date()}, "
        f"mode={conflict_mode}, dry_run={dry_run}"
    )

    if backup_id:
        logger.info(f"Restoring from backup snapshot: {backup_id}")

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

            logger.info(f"=== RESTORE TABLE: {table} (PK: {primary_key}) ===")

            # Get current DB schema
            db_columns = set(pg_get_columns(conn, table))
            logger.debug(f"Target table columns: {sorted(db_columns)}")

            # Iterate through monthly chunks
            curr = month_floor(start_date)
            cutoff = end_date

            while curr < cutoff:
                next_month = add_months(curr, 1)
                chunk_spec = ChunkSpec(table=table, start=curr, end=next_month)

                logger.info(f"Processing chunk: {chunk_spec}")

                # Get local file paths
                final_parquet, _, _, manifest_path = chunk_paths(
                    config.archive.base_dir, chunk_spec
                )

                # Determine if we need to download from S3
                need_download = False
                if backup_id:
                    # Always download for specific backup to ensure correct version
                    need_download = True
                elif not final_parquet.exists():
                    need_download = True

                # Download from S3 if needed
                if need_download:
                    if s3:
                        if backup_id:
                            # Reconstruct key for backup mode
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

                        logger.info(f"Downloading from s3://{config.s3.bucket}/{s3_key}")

                        if not dry_run:
                            try:
                                # Ensure parent directory exists
                                final_parquet.parent.mkdir(parents=True, exist_ok=True)
                                s3.download_file(config.s3.bucket, s3_key, final_parquet.as_posix())

                                # Verify checksum after download
                                try:
                                    head = s3.head_object(Bucket=config.s3.bucket, Key=s3_key)
                                    expected_sha = head.get("Metadata", {}).get("sha256", "")
                                    if expected_sha:
                                        from backparq.parquet import sha256_file

                                        actual_sha = sha256_file(final_parquet)
                                        if actual_sha != expected_sha:
                                            logger.error(
                                                f"Checksum mismatch for {s3_key}: "
                                                f"expected {expected_sha[:16]}..., got {actual_sha[:16]}..."
                                            )
                                            final_parquet.unlink(missing_ok=True)
                                            curr = next_month
                                            continue
                                        logger.debug(f"Checksum verified: {actual_sha[:16]}...")
                                except Exception as e:
                                    logger.warning(f"Could not verify checksum: {e}")

                                logger.info("Download successful")
                            except Exception as e:
                                logger.warning(f"Failed to download: {e}")
                                curr = next_month
                                continue
                        else:
                            logger.info("DRY RUN: would download from S3")
                    else:
                        logger.warning("Skipping chunk - no S3 connection and file missing locally")
                        curr = next_month
                        continue

                # Restore data from parquet file
                if dry_run:
                    logger.info(
                        f"DRY RUN: would restore {final_parquet} -> DB (mode: {conflict_mode})"
                    )
                else:
                    if not final_parquet.exists():
                        logger.warning(f"Skipping restore - file missing: {final_parquet}")
                        curr = next_month
                        continue

                    logger.info(f"Restoring {final_parquet}...")
                    try:
                        table_arrow = read_chunk(final_parquet)

                        # Handle schema evolution - filter to common columns
                        parquet_cols = set(table_arrow.column_names)
                        common_cols = list(parquet_cols.intersection(db_columns))

                        if len(common_cols) < len(parquet_cols):
                            dropped = parquet_cols - db_columns
                            logger.warning(
                                f"Ignoring columns present in Parquet but missing in DB: {dropped}"
                            )

                        if not common_cols:
                            logger.error(
                                "No common columns between Parquet and DB - skipping chunk"
                            )
                            curr = next_month
                            continue

                        # Filter to common columns
                        table_arrow_filtered = table_arrow.select(common_cols)

                        # Insert data
                        inserted = insert_arrow_table_to_pg(
                            conn=conn,
                            table=table,
                            arrow_table=table_arrow_filtered,
                            conflict_mode=conflict_mode,
                            primary_key=primary_key,
                        )
                        logger.info(f"Restored {inserted} rows")

                    except Exception as e:
                        logger.error(f"Failed to restore chunk: {e}")

                curr = next_month

    finally:
        conn.close()
        logger.info("Restore complete")
