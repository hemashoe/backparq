import datetime as dt
from pathlib import Path
from typing import Optional

from backparq.config import BackparqConfig, ensure_under_base_dir
from backparq.db import connect_pg, insert_arrow_table_to_pg, add_months, month_floor, pg_get_columns
from backparq.parquet import read_chunk, load_manifest
from backparq.s3 import s3_client_from_config
from backparq.archive import chunk_paths, s3_key_for_chunk
from backparq.db import ChunkSpec

def restore_tables(
    config: BackparqConfig,
    start_date: dt.datetime,
    end_date: dt.datetime,
    dry_run: bool = False,
    conflict_mode: str = "do_nothing",
    backup_id: Optional[str] = None,
) -> None:
    ensure_under_base_dir(config.archive.base_dir)

    conn = connect_pg(config.database)
    s3 = None
    if config.s3.bucket:
        try:
            test_s3_connection(config.s3)
            s3 = s3_client_from_config(config.s3)
        except Exception:
            pass

    try:
        for table in config.archive.tables:
            print(f"\n=== RESTORE TABLE: {table} ===")
            
            db_columns = set(pg_get_columns(conn, table))
            print(f"Target table columns: {sorted(list(db_columns))}")

            curr = month_floor(start_date)
            cutoff = end_date 
            
            while curr < cutoff:
                next_month = add_months(curr, 1)
                
                chunk_spec = ChunkSpec(table=table, start=curr, end=next_month)
                
                # Check for file
                # TODO: If backup_id is set, we use force S3 path logic?
                # Local paths are ignorant of backup_id currently.
                # If we download a specific backup, we should probably overwrite local cache or use temp?
                # For now, let's just use standard local path, implied overwrite if we download.
                
                final_parquet, _, _, manifest_path = chunk_paths(config.archive.base_dir, chunk_spec)
                
                # Logic: If backup_id is provided, we MUST download from that specific S3 path to ensure we get that version.
                # We cannot rely on local cache being correct.
                
                need_download = False
                if backup_id:
                     need_download = True
                elif not final_parquet.exists():
                     need_download = True

                if need_download:
                    if s3:
                        if backup_id:
                            # Reconstruct key for backup mode
                            # archive.py: f"{base_prefix}/backups/{run_id}/{chunk.table}/year={year:04d}/month={month:02d}/{name}"
                            year = curr.year
                            month = curr.month
                            name = f"{table}_{year:04d}-{month:02d}.parquet"
                            s3_key = f"{config.s3.prefix}/backups/{backup_id}/{table}/year={year:04d}/month={month:02d}/{name}"
                        else:
                            s3_key = s3_key_for_chunk(config.s3.prefix, chunk_spec, mode="offload")

                        print(f"Attempting download from s3://{config.s3.bucket}/{s3_key}")
                        if not dry_run:
                            try:
                                s3.download_file(config.s3.bucket, s3_key, final_parquet.as_posix())
                                print("Download successful.")
                            except Exception as e:
                                print(f"Failed to download: {e}")
                                curr = next_month
                                continue
                        else:
                            print("DRY RUN: would download from S3.")
                    else:
                        print("Skipping (no S3 configured or file missing).")
                        curr = next_month
                        continue

                # Read and Restore
                if dry_run:
                    print(f"DRY RUN: would restore {final_parquet} -> DB (Mode: {conflict_mode})")
                else:
                    if not final_parquet.exists():
                         print("Skipping restore (file missing).")
                         curr = next_month
                         continue

                    print(f"Restoring {final_parquet} ...")
                    try:
                        table_arrow = read_chunk(final_parquet)
                        
                        parquet_cols = set(table_arrow.column_names)
                        common_cols = list(parquet_cols.intersection(db_columns))
                        
                        if len(common_cols) < len(parquet_cols):
                            dropped = parquet_cols - db_columns
                            print(f"WARNING: Ignoring columns present in Parquet but missing in DB: {dropped}")
                        
                        if not common_cols:
                            print("ERROR: No common columns between Parquet and DB. Skipping chunk.")
                            curr = next_month
                            continue
                            
                        table_arrow_filtered = table_arrow.select(common_cols)

                        inserted = insert_arrow_table_to_pg(
                            conn=conn, 
                            table=table, 
                            arrow_table=table_arrow_filtered, 
                            conflict_mode=conflict_mode,
                            primary_key="id"
                        )
                        print(f"Restored {inserted} rows.")
                    except Exception as e:
                        print(f"Failed to restore chunk: {e}")

                curr = next_month

    finally:
        conn.close()
