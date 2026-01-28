"""Archive PostgreSQL tables to Parquet files with S3 upload."""

import datetime as dt
import json
import logging
import signal
import sys
import threading
import time
from pathlib import Path
from typing import Optional

from backparq.config import BackparqConfig
from backparq.console import (
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
from backparq.db import (
    ChunkSpec,
    connect_pg,
    delete_chunk_safely,
    export_chunk_to_parquet_streaming,
    list_chunks,
    pg_count_rows,
    validate_tables_exist,
)
from backparq.models import ArchiveResult, RunProgress, TableProgress
from backparq.parquet import (
    build_encryption_properties,
    load_manifest,
    safe_mkdir,
    sha256_file,
    validate_parquet_file,
    write_manifest,
    write_text,
)
from backparq.s3 import s3_client_from_config, s3_upload_file, s3_verify_object_sha256

logger = logging.getLogger(__name__)

_shutdown_requested = threading.Event()
_result_lock = threading.Lock()
_current_result = ArchiveResult()


def request_shutdown():
    _shutdown_requested.set()


def is_shutdown_requested() -> bool:
    return _shutdown_requested.is_set()


def _setup_signal_handlers():
    def handler(sig, frame):
        logger.warning(f"Received signal {sig}, initiating graceful shutdown...")
        request_shutdown()

    signal.signal(signal.SIGINT, handler)
    signal.signal(signal.SIGTERM, handler)


def _update_result(chunks=0, rows=0, bytes_up=0, error=None):
    with _result_lock:
        _current_result.chunks_archived += chunks
        _current_result.rows_archived += rows
        _current_result.bytes_uploaded += bytes_up
        if error:
            _current_result.errors.append(error)


def chunk_paths(base_dir: Path, chunk: ChunkSpec) -> tuple[Path, Path, Path, Path]:
    """Generate file paths for a chunk."""
    year, month = chunk.start.year, chunk.start.month
    safe_table = chunk.table.replace(".", "_")
    name = f"{safe_table}_{year:04d}-{month:02d}.parquet"

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
    name = f"{safe_table}_{year:04d}-{month:02d}.parquet"

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
    s3,
    do_upload: bool,
    extra_args: dict,
    encryption_properties,
    run_id: Optional[str] = None,
) -> dict:
    """Process a single chunk. Returns stats dict."""
    if is_shutdown_requested():
        return {"rows": 0, "bytes": 0}

    conn = connect_pg(config.database)
    try:
        return _process_chunk_impl(
            chunk, config, conn, s3, do_upload, extra_args, encryption_properties, run_id
        )
    finally:
        conn.close()


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
        logger.info(f"DRY RUN: Would archive {db_rows} rows from {chunk}")
        return stats

    db_rows = pg_count_rows(conn, chunk.table, chunk.start, chunk.end, config.archive.order_by)
    if db_rows == 0:
        return stats

    logger.debug(f"Chunk {chunk}: {db_rows} rows")

    # Check existing manifest
    existing = load_manifest(manifest_path)
    already_done = False

    if existing and not config.archive.overwrite:
        expected = existing.get("exported_rows")
        sha = existing.get("sha256", "")

        if final_parquet.exists() and expected is not None:
            try:
                parquet_rows = validate_parquet_file(final_parquet, int(expected))
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
            config.archive.order_by,
            inprogress_parquet,
            config.parquet.row_group_size,
            config.parquet.compression,
            encryption_properties,
        )

        if row_count == 0:
            if inprogress_parquet.exists():
                inprogress_parquet.unlink()
            return stats

        inprogress_parquet.rename(final_parquet)
        sha = sha256_file(final_parquet)
        write_text(sha_path, sha)
        stats["rows"] = row_count
        stats["bytes"] = final_parquet.stat().st_size
    else:
        sha = existing.get("sha256", sha256_file(final_parquet))
        stats["bytes"] = final_parquet.stat().st_size if final_parquet.exists() else 0

    # Upload
    uploaded = False
    if do_upload and s3:
        if not s3_verify_object_sha256(s3, config.s3.bucket, s3_key, sha):
            metadata = {
                "sha256": sha,
                "rows": str(stats["rows"] or existing.get("exported_rows", 0)),
            }
            s3_upload_file(s3, str(final_parquet), config.s3.bucket, s3_key, extra_args, metadata)
            uploaded = True
            logger.debug(f"Uploaded: {s3_key}")
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
        if delete_chunk_safely(
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
    s3,
    extra_args: dict,
    run_id: Optional[str],
    progress,
    task_id,
) -> dict:
    """Process all chunks for a table."""
    table_stats = {"chunks": 0, "rows": 0, "bytes": 0}

    if is_shutdown_requested():
        return table_stats

    conn = connect_pg(config.database)
    try:
        cutoff = config.archive.cutoff_exclusive
        if config.archive.mode == "backup" or cutoff is None:
            cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)

        chunks = list_chunks(conn, table, cutoff, config.archive.order_by)
    finally:
        conn.close()

    if not chunks:
        logger.debug(f"No data for {table}")
        return table_stats

    encryption_properties = build_encryption_properties(config.parquet)
    do_upload = bool(config.s3.bucket)

    for chunk in chunks:
        if is_shutdown_requested():
            break

        try:
            stats = _process_chunk(
                chunk, config, s3, do_upload, extra_args, encryption_properties, run_id
            )
            table_stats["chunks"] += 1
            table_stats["rows"] += stats["rows"]
            table_stats["bytes"] += stats["bytes"]
            _update_result(chunks=1, rows=stats["rows"], bytes_up=stats["bytes"])
        except Exception as e:
            logger.error(f"Chunk error {chunk}: {e}")
            _update_result(error=str(e))

        progress.advance(task_id)

    return table_stats


def _write_progress(path: Path, run_progress: RunProgress):
    """Write progress file."""
    with open(path, "w") as f:
        json.dump(run_progress.to_dict(), f, indent=2)


def archive_tables(config: BackparqConfig, show_stats: bool = False) -> ArchiveResult:
    """Archive all configured tables."""
    global _current_result
    _current_result = ArchiveResult()
    start_time = time.time()

    _setup_signal_handlers()

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

    # Count total chunks
    total_chunks = 0
    table_chunks = {}

    conn = connect_pg(config.database)
    try:
        for table in table_names:
            cutoff = config.archive.cutoff_exclusive or dt.datetime.now(
                dt.timezone.utc
            ) + dt.timedelta(days=1)
            chunks = list_chunks(conn, table, cutoff, config.archive.order_by)
            table_chunks[table] = len(chunks)
            total_chunks += len(chunks)
            run_progress.tables[table] = TableProgress(status="pending", chunks_total=len(chunks))
    finally:
        conn.close()

    if total_chunks == 0:
        print_warning("No data to archive")
        return _current_result

    console.print(f"Found [bold]{total_chunks}[/bold] chunks across {len(table_names)} tables\n")

    # Process with progress bar
    with create_progress() as progress:
        main_task = progress.add_task("Archiving", total=total_chunks)

        for table in table_names:
            if is_shutdown_requested():
                break

            run_progress.tables[table].status = "in_progress"
            _write_progress(progress_file, run_progress)

            try:
                stats = _process_table(table, config, s3, extra_args, run_id, progress, main_task)
                run_progress.tables[table].status = "completed"
                run_progress.tables[table].chunks_complete = stats["chunks"]
                run_progress.tables[table].rows_archived = stats["rows"]
                _current_result.tables_processed += 1

                logger.info(
                    f"Completed {table}: {stats['chunks']} chunks, {format_count(stats['rows'])} rows"
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
    else:
        print_error(f"Archive completed with {len(_current_result.errors)} errors")

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

    if is_shutdown_requested():
        print_warning("Archive interrupted")
        sys.exit(130)

    return _current_result
