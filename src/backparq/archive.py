import datetime as dt
import time
from pathlib import Path

from backparq.config import BackparqConfig, ensure_under_base_dir
from backparq.db import (
    ChunkSpec,
    connect_pg,
    delete_chunk_safely,
    export_chunk_to_parquet_streaming,
    list_chunks,
    pg_count_rows,
    test_pg_connection,
)
from backparq.parquet import (
    build_encryption_properties,
    load_manifest,
    safe_mkdir,
    sha256_file,
    validate_parquet_file,
    write_manifest,
    write_text,
)
from backparq.s3 import (
    s3_client_from_config,
    s3_upload_file,
    s3_verify_object_sha256,
    test_s3_connection,
)


def chunk_paths(base_dir: Path, chunk: ChunkSpec) -> tuple[Path, Path, Path, Path]:
    ensure_under_base_dir(base_dir)
    year = chunk.start.year
    month = chunk.start.month
    out_dir = base_dir / "parquet" / chunk.table / f"year={year:04d}" / f"month={month:02d}"
    safe_mkdir(out_dir)

    name = f"{chunk.table}_{year:04d}-{month:02d}.parquet"
    final_parquet = out_dir / name
    inprogress_parquet = out_dir / f"{name}.inprogress"
    sha_path = out_dir / f"{name}.sha256"
    manifest_path = out_dir / f"{name}.manifest.json"
    return final_parquet, inprogress_parquet, sha_path, manifest_path


def s3_key_for_chunk(base_prefix: str, chunk: ChunkSpec, mode: str, run_id: Optional[str] = None) -> str:
    year = chunk.start.year
    month = chunk.start.month
    name = f"{chunk.table}_{year:04d}-{month:02d}.parquet"
    
    if mode == "backup":
        if not run_id:
             raise ValueError("run_id required for backup mode")
        # Structure: prefix/backups/RUN_ID/table/...
        return f"{base_prefix}/backups/{run_id}/{chunk.table}/year={year:04d}/month={month:02d}/{name}"
    else:
        # Structure: prefix/archive/table/...
        return f"{base_prefix}/archive/{chunk.table}/year={year:04d}/month={month:02d}/{name}"


def _s3_extra_args(config) -> dict:
    extra_args: dict = {}
    if config.sse:
        extra_args["ServerSideEncryption"] = config.sse
    if config.sse == "aws:kms" and config.kms_key_id:
        extra_args["SSEKMSKeyId"] = config.kms_key_id
    return extra_args


from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm

def _process_table(table: str, config: BackparqConfig, s3, extra_args, run_id: Optional[str] = None) -> None:
    conn = connect_pg(config.database)
    chunks = []
    try:
        # If backup mode, we force cutoff to be NOW (full snapshot)
        if config.archive.mode == "backup":
             cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        else:
            # Offload mode uses configured cutoff
            cutoff = config.archive.cutoff_exclusive
            if cutoff is None:
                 cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)
        
        chunks = list_chunks(conn, table, cutoff)
    finally:
        conn.close()
        
    if not chunks:
        return

    encryption_properties = build_encryption_properties(config.parquet)
    do_upload = bool(config.s3.bucket)

    desc = f"Table {table}"
    
    concurrency = config.archive.chunk_concurrency
    
    if concurrency > 1:
        desc = f"Table {table} (Parallel x{concurrency})"
        with ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = [
                executor.submit(_process_chunk, c, config, s3, do_upload, extra_args, encryption_properties, run_id)
                for c in chunks
            ]
            for f in tqdm(as_completed(futures), total=len(chunks), desc=desc, leave=False):
                try:
                    f.result()
                except Exception as e:
                    print(f"ERROR processing chunk for table {table}: {e}")
    else:
        for chunk in tqdm(chunks, desc=desc, leave=False, disable=None):
            try:
                _process_chunk(
                    chunk, config, s3, do_upload, extra_args, encryption_properties, run_id
                )
            except Exception as e:
                print(f"ERROR processing chunk {chunk} for table {table}: {e}")


def _process_chunk(chunk, config, s3, do_upload, extra_args, encryption_properties, run_id: Optional[str] = None):
    # Each chunk gets its own connection to be thread-safe
    conn = connect_pg(config.database)
    try:
        _process_chunk_impl(chunk, config, conn, s3, do_upload, extra_args, encryption_properties, run_id)
    finally:
        conn.close()

def _process_chunk_impl(chunk, config, conn, s3, do_upload, extra_args, encryption_properties, run_id: Optional[str] = None):
    # For local storage, we can keep the same structure or use run_id too?
    # Ideally local storage mirrors transparency. 
    # But simple "table_YYYY-MM" might overwrite if we backup same month twice?
    # In Backup mode, we probably want local isolation too, primarily for collision avoidance.
    # But currently chunk_paths uses base_dir directly.
    # Let's keep local paths simple for now (cache), assuming sequential runs or different base_dirs.
    
    year, month = chunk.start.year, chunk.start.month
    final_parquet, inprogress_parquet, sha_path, manifest_path = chunk_paths(
        config.archive.base_dir, chunk
    )
    
    # 1. Export
    # ... (Standard export logic)
    
    if final_parquet.exists():
         # Re-use existing file if checksum matches?
         # in backup mode, we might re-upload the same file to a different S3 key.
         pass
    else:
         # Export...
         rows = export_chunk_to_parquet_streaming(
            conn, chunk.table, chunk.start, chunk.end, 
            inprogress_parquet, config.archive.order_by, config.archive.fetch_size,
            config.parquet.compression, encryption_properties
         )
         if rows == 0:
             if inprogress_parquet.exists(): inprogress_parquet.unlink()
             return
         inprogress_parquet.replace(final_parquet)

    sha256 = sha256_file(final_parquet)
    write_text(sha_path, sha256)
    
    # 2. Upload
    if do_upload and s3:
        key = s3_key_for_chunk(config.s3.prefix, chunk, config.archive.mode, run_id)
        s3_upload_file(s3, final_parquet, config.s3.bucket, key, sha256, extra_args)

        # Verify
        # ...
        
    # 3. Delete (Only in Offload Mode)
    if config.archive.mode == "offload" and config.archive.perform_delete:
         deleted = delete_chunk_safely(
             conn, chunk.table, chunk.start, chunk.end, config.archive.delete_batch_size
         )
    # Check existing
    existing_manifest = load_manifest(manifest_path)
    already_archived = False
    already_verified = False

    if existing_manifest and not config.archive.overwrite:
        if final_parquet.exists():
            try:
                parquet_rows = validate_parquet_file(
                    final_parquet, int(existing_manifest.get("exported_rows", -1))
                )
            except Exception:
                parquet_rows = -1
        else:
            parquet_rows = -1

        sha = existing_manifest.get("sha256", "")
        expected_rows = existing_manifest.get("exported_rows", None)

        ok_rows = (
            expected_rows is not None
            and parquet_rows == expected_rows
            and expected_rows == db_rows
        )
        ok_s3 = True
        if do_upload and s3 is not None:
            mb = existing_manifest.get("s3_bucket", config.s3.bucket)
            mk = existing_manifest.get("s3_key", s3_key)
            ok_s3 = bool(sha) and s3_verify_object_sha256(s3, mb, mk, sha)

        already_archived = ok_rows and final_parquet.exists()
        already_verified = already_archived and ok_s3

        if already_archived and not config.archive.perform_delete:
             return

        if config.archive.perform_delete and already_verified:
             # Determine if row count matches (safe delete check)
             pass # proceed to delete block
    
    # Export & Upload
    if not (config.archive.perform_delete and already_verified) and not already_archived:
        # Cleanup
        if inprogress_parquet.exists(): inprogress_parquet.unlink()
        if final_parquet.exists() and config.archive.overwrite: final_parquet.unlink()
        if sha_path.exists() and config.archive.overwrite: sha_path.unlink()
        if manifest_path.exists() and config.archive.overwrite: manifest_path.unlink()

        # Atomic Export
        exported = export_chunk_to_parquet_streaming(
            conn=conn,
            table=chunk.table,
            start=chunk.start,
            end=chunk.end,
            parquet_path=inprogress_parquet,
            order_by=config.archive.order_by,
            fetch_size=config.archive.fetch_size,
            compression=config.parquet.compression,
            encryption_properties=encryption_properties,
        )
        
        if exported != db_rows:
             raise RuntimeError(f"Export mismatch: {exported} != {db_rows}")

        validate_parquet_file(inprogress_parquet, exported)
        inprogress_parquet.replace(final_parquet)
        validate_parquet_file(final_parquet, exported)

        sha = sha256_file(final_parquet)
        write_text(sha_path, sha + "\n")

        uploaded_and_verified = False
        if do_upload:
            s3_upload_file(
                s3=s3,
                local_path=final_parquet,
                bucket=config.s3.bucket,
                key=s3_key,
                sha256_hex=sha,
                extra_args=extra_args or None,
            )
            uploaded_and_verified = s3_verify_object_sha256(s3, config.s3.bucket, s3_key, sha)
            if not uploaded_and_verified:
                raise RuntimeError("S3 verification failed.")
        
        manifest = {
            "table": chunk.table,
            "chunk_start": chunk.start.isoformat(),
            "chunk_end": chunk.end.isoformat(),
            "cutoff_exclusive": config.archive.cutoff_exclusive.isoformat() if config.archive.cutoff_exclusive else "FULL_TABLE",
            "exported_rows": exported,
            "db_rows_at_export_time": db_rows,
            "parquet_path": final_parquet.as_posix(),
            "sha256": sha,
            "s3_bucket": config.s3.bucket if do_upload else "",
            "s3_key": s3_key if do_upload else "",
            "uploaded_and_verified": uploaded_and_verified if do_upload else False,
            "created_at_utc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "order_by": config.archive.order_by,
            "compression": config.parquet.compression,
            "fetch_size": config.archive.fetch_size,
            "encryption_enabled": config.parquet.encryption.enabled,
        }
        write_manifest(manifest_path, manifest)

    # Delete
    if config.archive.perform_delete:
         # Simplified double-check before delete
         manifest = load_manifest(manifest_path)
         if not manifest: raise RuntimeError("Missing manifest for delete.")
         
         exported_rows = int(manifest.get("exported_rows", -1))
         if do_upload:
              mk = manifest.get("s3_key", s3_key)
              sha = manifest.get("sha256", "")
              if not s3_verify_object_sha256(s3, config.s3.bucket, mk, sha):
                   raise RuntimeError("S3 verification failed before delete.")
         
         db_rows_now = pg_count_rows(conn, chunk.table, chunk.start, chunk.end)
         if db_rows_now != exported_rows:
              raise RuntimeError(f"DB count changed ({db_rows_now} != {exported_rows}). Aborting delete.")

         deleted = delete_chunk_safely(
             conn=conn, table=chunk.table, start=chunk.start, end=chunk.end, batch_size=config.archive.delete_batch_size
         )
         if deleted != exported_rows:
              print(f"WARNING: Deleted {deleted} != Exported {exported_rows}")


def archive_tables(config: BackparqConfig) -> None:
    ensure_under_base_dir(config.archive.base_dir)

    do_upload = bool(config.s3.bucket)
    s3 = None
    extra_args = _s3_extra_args(config.s3)

    if config.archive.dry_run:
        print("DRY RUN: will not write/upload/delete.\n")

    # Tests connections (main thread)
    test_pg_connection(config.database)
    if do_upload:
        test_s3_connection(config.s3)
        # We assume s3 client is thread-safe OR we create one per thread.
        # boto3 clients are thread safe.
        s3 = s3_client_from_config(config.s3)

    # Parallel Execution
    max_workers = config.archive.concurrency
    print(f"Starting archive for {len(config.archive.tables)} tables with concurrency={max_workers}...")

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_process_table, table, config, s3, extra_args): table 
            for table in config.archive.tables
        }
        
        for future in as_completed(futures):
            table = futures[future]
            try:
                future.result()
            except Exception as exc:
                print(f"Table {table} failed: {exc}")

    print("\nAll done.")
