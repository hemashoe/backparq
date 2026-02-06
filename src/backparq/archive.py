"""Archive PostgreSQL tables to Parquet files with S3 upload."""

from __future__ import annotations

import concurrent.futures
import datetime as dt
import json
import logging
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from backparq.config import BackparqConfig
from backparq.db import (
    ChunkSpec,
    connect_pg,
    delete_chunk_with_verification,
    export_chunk_to_parquet_streaming,
    list_chunks,
    pg_count_rows,
    vacuum_table,
    validate_tables_exist,
)
from backparq.models import ArchiveResult, RunProgress, TableProgress
from backparq.utils.console import (
    console,
    create_progress,
    format_count,
    format_duration,
    format_size,
    print_error,
    print_header,
    print_stats,
    print_success,
    print_warning,
)
from backparq.utils.notifications import send_notification

_result_lock = threading.Lock()
_current_result = ArchiveResult()


from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.storage.s3 import upload_file as s3_upload_file
from backparq.storage.s3 import verify_checksum as s3_verify_object_sha256
from backparq.utils.lock import Lock as BackparqLock
from backparq.utils.lock import LockError
from backparq.utils.logging import log_with_data

logger = logging.getLogger(__name__)


def _update_result(chunks=0, rows=0, bytes_up=0, error=None):
    with _result_lock:
        _current_result.chunks_archived += chunks
        _current_result.rows_archived += rows
        _current_result.bytes_uploaded += bytes_up
        if error:
            _current_result.errors.append(error)


def _get_chunk_filename(chunk: ChunkSpec, suffix: str = "") -> str:
    """Generate unique filename for a chunk."""
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")
    # Include day/time in filename to avoid collisions between chunks in same month
    # and collisions between runs
    start_str = chunk.start.strftime("%Y%m%d%H%M%S")
    return f"{safe_table}_{year:04d}-{month:02d}_{start_str}{suffix}.parquet"


def chunk_paths(base_dir: Path, chunk: ChunkSpec) -> tuple[Path, Path, Path, Path]:
    """Generate file paths for a chunk."""
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")

    # Use a random suffix or similar for local temp file to avoid collision if parallel
    import uuid

    name = _get_chunk_filename(chunk, suffix=f"_{uuid.uuid4().hex[:8]}")

    chunk_dir = base_dir / "parquet" / safe_table / f"year={year:04d}" / f"month={month:02d}"
    safe_mkdir(chunk_dir)

    final = chunk_dir / name
    inprogress = chunk_dir / f"{name}.inprogress"
    sha_path = chunk_dir / f"{name}.sha256"
    manifest = chunk_dir / f"{name}.manifest.json"

    return final, inprogress, sha_path, manifest


def s3_key_for_chunk(
    base_prefix: str, chunk: ChunkSpec, mode: str, run_id: Optional[str] = None
) -> str:
    """Generate S3 key for a chunk."""
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")

    # For S3, we want stable but unique names. run_id helps if provided.
    suffix = f"_{run_id}" if run_id else ""
    name = _get_chunk_filename(chunk, suffix=suffix)

    if mode == "backup":
        if not run_id:
            raise ValueError("run_id required for backup mode")
        return (
            f"{base_prefix}/backups/{run_id}/{safe_table}/year={year:04d}/month={month:02d}/{name}"
        )
    return f"{base_prefix}/archive/{safe_table}/year={year:04d}/month={month:02d}/{name}"


def _s3_extra_args(config) -> dict:
    """Build S3 upload arguments."""
    args = {}
    if config.sse:
        args["ServerSideEncryption"] = config.sse
    if config.sse == "aws:kms" and config.kms_key_id:
        args["SSEKMSKeyId"] = config.kms_key_id
    return args


def _process_chunk(
    chunk: ChunkSpec,
    config: BackparqConfig,
    pool,
    s3,
    do_upload: bool,
    extra_args: dict,
    encryption_properties,
    run_id: Optional[str] = None,
    shutdown_event: Optional[threading.Event] = None,
) -> dict:
    """Process a single chunk. Returns stats dict."""
    if shutdown_event and shutdown_event.is_set():
        return {"rows": 0, "bytes": 0}

    with pool.connection() as conn:
        return _process_chunk_impl(
            chunk, config, conn, s3, do_upload, extra_args, encryption_properties, run_id
        )


def _process_chunk_impl(
    chunk: ChunkSpec,
    config: BackparqConfig,
    conn,
    s3,
    do_upload: bool,
    extra_args: dict,
    encryption_properties,
    run_id: Optional[str] = None,
) -> dict:
    """Core chunk processing logic."""
    stats = {"rows": 0, "bytes": 0}

    final_parquet, inprogress_parquet, sha_path, manifest_path = chunk_paths(
        config.archive.base_dir, chunk
    )
    s3_key = s3_key_for_chunk(config.s3.prefix, chunk, config.archive.mode, run_id)

    if config.archive.dry_run:
        db_rows = pg_count_rows(conn, chunk.table, chunk.start, chunk.end, config.archive.order_by)
        log_with_data(
            logger,
            logging.INFO,
            f"DRY RUN: Would archive chunk {chunk}",
            table=chunk.table,
            rows=db_rows,
            start=str(chunk.start),
            end=str(chunk.end),
        )
        return stats

    db_rows = pg_count_rows(conn, chunk.table, chunk.start, chunk.end, config.archive.order_by)
    if db_rows == 0:
        return stats

    log_with_data(
        logger,
        logging.DEBUG,
        "Processing chunk",
        table=chunk.table,
        rows=db_rows,
        start=str(chunk.start),
        end=str(chunk.end),
    )

    # Check existing manifest
    existing = load_manifest(manifest_path)
    already_done = False

    if existing and not config.archive.overwrite:
        expected = existing.get("exported_rows")
        sha = existing.get("sha256", "")

        if final_parquet.exists() and expected is not None:
            try:
                parquet_rows = validate_file(final_parquet, int(expected))
                already_done = parquet_rows == expected == db_rows
            except Exception:
                already_done = False

        if do_upload and s3 and sha and already_done:
            already_done = s3_verify_object_sha256(
                s3, config.s3.bucket, existing.get("s3_key", s3_key), sha
            )

        if already_done and not config.archive.perform_delete:
            logger.debug(f"Chunk {chunk} already done")
            return stats

    # Export
    if not already_done:
        if config.archive.overwrite:
            for f in [final_parquet, inprogress_parquet, sha_path, manifest_path]:
                if f.exists():
                    f.unlink()

        if inprogress_parquet.exists():
            inprogress_parquet.unlink()

        row_count = export_chunk_to_parquet_streaming(
            conn,
            chunk.table,
            chunk.start,
            chunk.end,
            inprogress_parquet,
            config.archive.order_by,
            config.archive.fetch_size,
            config.parquet.row_group_size,
            config.parquet.compression,
            encryption_properties,
            masking=config.archive.get_table_config(chunk.table).masking,
        )

        if row_count == 0:
            if inprogress_parquet.exists():
                inprogress_parquet.unlink()
            return stats

        inprogress_parquet.rename(final_parquet)
        sha = compute_sha256(final_parquet)
        sha_path.write_text(sha)
        stats["rows"] = row_count
        stats["bytes"] = final_parquet.stat().st_size
    else:
        sha = existing.get("sha256", compute_sha256(final_parquet))
        stats["bytes"] = final_parquet.stat().st_size if final_parquet.exists() else 0

    # Upload
    uploaded = False
    if do_upload and s3:
        if not s3_verify_object_sha256(s3, config.s3.bucket, s3_key, sha):
            upload_start = time.time()
            s3_upload_file(
                s3,
                final_parquet,
                config.s3.bucket,
                s3_key,
                sha,
                extra_args,
                metadata={"rows": stats["rows"]},
            )
            upload_duration = time.time() - upload_start
            uploaded = True
            log_with_data(
                logger,
                logging.DEBUG,
                "Uploaded chunk to S3",
                s3_key=s3_key,
                size_bytes=stats["bytes"],
                duration_seconds=round(upload_duration, 2),
            )
        else:
            uploaded = True

    # Write manifest
    manifest = {
        "table": chunk.table,
        "start": chunk.start.isoformat(),
        "end": chunk.end.isoformat(),
        "exported_rows": stats["rows"] or existing.get("exported_rows"),
        "sha256": sha,
        "s3_key": s3_key if do_upload else None,
        "s3_verified": uploaded,
        "archived_at": dt.datetime.now(dt.timezone.utc).isoformat(),
    }
    write_manifest(manifest_path, manifest)

    # Delete
    if config.archive.perform_delete and uploaded:
        if delete_chunk_with_verification(
            conn,
            chunk.table,
            sha,
            config.s3.bucket,
            s3_key,
            s3,
            chunk.start,
            chunk.end,
            config.archive.order_by,
            config,
        ):
            logger.info(f"Deleted source data: {chunk}")

    return stats


def _process_table(
    table: str,
    config: BackparqConfig,
    pool,
    s3,
    extra_args: dict,
    run_id: Optional[str],
    progress,
    task_id,
    shutdown_event: threading.Event,
) -> dict:
    """Process all chunks for a table."""
    table_stats = {"chunks": 0, "rows": 0, "bytes": 0}

    if shutdown_event.is_set():
        return table_stats

    # List chunks using pool connection
    with pool.connection() as conn:
        cutoff = config.archive.cutoff_exclusive
        if config.archive.mode == "backup" or cutoff is None:
            cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)

        chunks = list_chunks(conn, table, cutoff, config.archive.order_by)

    if not chunks:
        logger.debug(f"No data for {table}")
        return table_stats

    encryption_properties = build_encryption(config.parquet)
    do_upload = bool(config.s3.bucket)

    max_workers = config.archive.chunk_concurrency

    if max_workers > 1:
        logger.info(f"Processing {table} with {max_workers} threads")
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {}
            for chunk in chunks:
                if shutdown_event.is_set():
                    break

                future = executor.submit(
                    _process_chunk,
                    chunk,
                    config,
                    pool,
                    s3,
                    do_upload,
                    extra_args,
                    encryption_properties,
                    run_id,
                    shutdown_event,
                )
                futures[future] = chunk

            for future in concurrent.futures.as_completed(futures):
                chunk = futures[future]
                try:
                    stats = future.result()
                    table_stats["chunks"] += 1
                    table_stats["rows"] += stats["rows"]
                    table_stats["bytes"] += stats["bytes"]
                    _update_result(chunks=1, rows=stats["rows"], bytes_up=stats["bytes"])
                except Exception as e:
                    logger.error(f"Chunk error {chunk}: {e}")
                    _update_result(error=str(e))

                progress.advance(task_id)
    else:
        # Sequential processing
        for chunk in chunks:
            if shutdown_event.is_set():
                break

            try:
                stats = _process_chunk(
                    chunk,
                    config,
                    pool,
                    s3,
                    do_upload,
                    extra_args,
                    encryption_properties,
                    run_id,
                    shutdown_event,
                )
                table_stats["chunks"] += 1
                table_stats["rows"] += stats["rows"]
                table_stats["bytes"] += stats["bytes"]
                _update_result(chunks=1, rows=stats["rows"], bytes_up=stats["bytes"])
            except Exception as e:
                logger.error(f"Chunk error {chunk}: {e}")
                _update_result(error=str(e))

            progress.advance(task_id)

    # Run VACUUM if configured and we are not shutting down
    # We only vacuum if we are in offload mode (perform_delete=True) or explicit vacuum request
    if config.archive.vacuum and not shutdown_event.is_set():
        try:
            with pool.autocommit_connection() as conn:
                vacuum_table(conn, table)
        except Exception as e:
            logger.error(f"Vacuum failed for {table}: {e}")

    return table_stats


def _write_progress(path: Path, run_progress: RunProgress):
    """Write progress file."""
    with open(path, "w") as f:
        json.dump(run_progress.to_dict(), f, indent=2)


from backparq.storage.parquet import (
    build_encryption,
    compute_sha256,
    load_manifest,
    safe_mkdir,
    validate_file,
    write_manifest,
)


def archive_tables(
    config: BackparqConfig,
    show_stats: bool = False,
    shutdown_event: Optional[threading.Event] = None,
) -> ArchiveResult:
    """Archive all configured tables."""
    global _current_result
    _current_result = ArchiveResult()
    start_time = time.time()

    # Use provided event or create a local one (that won't be triggered by signals unless passed in)
    shutdown = shutdown_event or threading.Event()

    print_header("BACKPARQ ARCHIVE")
    console.print(f"Mode: [bold]{config.archive.mode}[/bold]")
    console.print(f"Tables: {len(config.archive.tables)}")
    console.print(
        f"S3: s3://{config.s3.bucket}/{config.s3.prefix}"
        if config.s3.bucket
        else "S3: Not configured"
    )

    if config.archive.dry_run:
        print_warning("DRY RUN: No data will be modified")
    console.print()

    # Acquire lock to prevent concurrent runs
    lock = None
    if not config.archive.dry_run:
        try:
            lock = BackparqLock(config.archive.base_dir)
            lock.acquire()
            console.print("[dim]Lock acquired[/dim]")
        except LockError as e:
            print_error(str(e))
            _current_result.errors.append(str(e))
            return _current_result

    try:
        return _archive_tables_impl(config, show_stats, start_time, lock, shutdown)
    finally:
        if lock:
            lock.release()


def _archive_tables_impl(
    config: BackparqConfig,
    show_stats: bool,
    start_time: float,
    lock: Optional[BackparqLock] = None,
    shutdown_event: Optional[threading.Event] = None,
) -> ArchiveResult:
    """Internal implementation of archive_tables (runs while holding lock)."""
    global _current_result

    # Use provided event or create a local one (no signal handling when embedded)
    shutdown = shutdown_event or threading.Event()

    s3 = None
    extra_args = _s3_extra_args(config.s3)

    # Run ID for backup mode
    run_id = None
    if config.archive.mode == "backup":
        run_id = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d_%H%M%S")
        console.print(f"Run ID: [cyan]{run_id}[/cyan]")

    # S3 setup
    if config.s3.bucket:
        s3 = s3_client_from_config(config.s3)

    # Validate tables
    table_names = config.archive.table_names
    console.print(f"Validating {len(table_names)} tables...")

    validation_conn = connect_pg(config.database)
    try:
        missing = validate_tables_exist(validation_conn, table_names)
        if missing:
            print_error(f"Tables not found: {', '.join(missing)}")
            _current_result.errors.append(f"Missing tables: {', '.join(missing)}")
            return _current_result
    finally:
        validation_conn.close()

    print_success("All tables validated")
    console.print()

    # Progress tracking
    progress_file = config.archive.base_dir / "progress.json"
    run_progress = RunProgress(
        run_id=run_id or dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d_%H%M%S"),
        started_at=dt.datetime.now(dt.timezone.utc),
    )

    # Notify start
    send_notification(
        config.notifications,
        "archive_started",
        {
            "run_id": run_progress.run_id,
            "timestamp": run_progress.started_at.isoformat(),
            "mode": config.archive.mode,
            "tables_count": len(table_names),
        },
    )

    # Initialize connection pool
    from backparq.db.connection import ConnectionPool

    # Set max connections based on concurrency + extra
    pool_size = max(2, config.archive.chunk_concurrency + 2)

    with ConnectionPool(config.database, minconn=2, maxconn=pool_size) as pool:
        # Count total chunks
        total_chunks = 0
        table_chunks = {}

        with pool.connection() as conn:
            for table in table_names:
                cutoff = config.archive.cutoff_exclusive or dt.datetime.now(
                    dt.timezone.utc
                ) + dt.timedelta(days=1)
                chunks = list_chunks(conn, table, cutoff, config.archive.order_by)
                table_chunks[table] = len(chunks)
                total_chunks += len(chunks)
                run_progress.tables[table] = TableProgress(
                    status="pending", chunks_total=len(chunks)
                )

        if total_chunks == 0:
            print_warning("No data to archive")
            return _current_result

        console.print(
            f"Found [bold]{total_chunks}[/bold] chunks across {len(table_names)} tables\n"
        )

        # Process with progress bar
        with create_progress() as progress:
            main_task = progress.add_task("Archiving", total=total_chunks)

            for table in table_names:
                if shutdown.is_set():
                    break

                run_progress.tables[table].status = "in_progress"
                _write_progress(progress_file, run_progress)

                try:
                    stats = _process_table(
                        table, config, pool, s3, extra_args, run_id, progress, main_task, shutdown
                    )
                    run_progress.tables[table].status = "completed"
                    run_progress.tables[table].chunks_complete = stats["chunks"]
                    run_progress.tables[table].rows_archived = stats["rows"]
                    _current_result.tables_processed += 1

                    log_with_data(
                        logger,
                        logging.INFO,
                        "Completed table archive",
                        table=table,
                        chunks=stats["chunks"],
                        rows=stats["rows"],
                        bytes=stats["bytes"],
                    )
                except Exception as e:
                    run_progress.tables[table].status = "failed"
                    _current_result.errors.append(f"{table}: {e}")
                    logger.error(f"Table {table} failed: {e}")

    _write_progress(progress_file, run_progress)

    # Calculate duration
    _current_result.duration_seconds = time.time() - start_time

    # Print summary
    console.print()
    if _current_result.success:
        print_success("Archive complete")
        send_notification(
            config.notifications,
            "archive_success",
            {
                "run_id": run_progress.run_id,
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "duration_seconds": _current_result.duration_seconds,
                "chunks": _current_result.chunks_archived,
                "rows": _current_result.rows_archived,
                "bytes": _current_result.bytes_uploaded,
            },
        )
    else:
        print_error(f"Archive completed with {len(_current_result.errors)} errors")
        send_notification(
            config.notifications,
            "archive_failed",
            {
                "run_id": run_progress.run_id,
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "duration_seconds": _current_result.duration_seconds,
                "errors": _current_result.errors,
            },
        )

    if show_stats or _current_result.rows_archived > 0:
        print_stats(
            {
                "Tables processed": _current_result.tables_processed,
                "Chunks archived": _current_result.chunks_archived,
                "Rows archived": format_count(_current_result.rows_archived),
                "Data uploaded": format_size(_current_result.bytes_uploaded),
                "Duration": format_duration(_current_result.duration_seconds),
                "Throughput": f"{_current_result.rows_per_second:.0f} rows/sec",
            }
        )

    if shutdown.is_set():
        print_warning("Archive interrupted")
        send_notification(
            config.notifications,
            "archive_failed",
            {
                "run_id": _current_result.run_id
                if hasattr(_current_result, "run_id")
                else "unknown",
                "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(),
                "error": "Interrupted by user",
            },
        )
        sys.exit(130)

    return _current_result
