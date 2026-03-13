from __future__ import annotations

import datetime as dt
import hashlib
import json
import logging
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any

from backparq.adapters.catalog import Catalog, ChunkState, RunStatus
from backparq.config import BackparqConfig, S3Config
from backparq.db import ConnectionPool, list_chunks, validate_tables_exist
from backparq.db.operations import ChunkSpec, pg_get_max_id
from backparq.models import ArchiveResult, RestoreResult, VerifyResult
from backparq.operations import delete_chunk, export_chunk, upload_chunk
from backparq.operations.restore_op import restore_chunk
from backparq.operations.verify_op import verify_chunk
from backparq.primitives import parse_iso_datetime
from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.utils.console import console, create_progress, print_header, print_success
from backparq.utils.lock import AdvisoryLock
from backparq.utils.notifications import send_notification

logger = logging.getLogger(__name__)


def _s3_extra_args(config: S3Config) -> dict[str, Any]:
    """Build S3 extra args for encryption."""
    args: dict[str, Any] = {}
    if config.sse:
        args["ServerSideEncryption"] = config.sse
    if config.sse == "aws:kms" and config.kms_key_id:
        args["SSEKMSKeyId"] = config.kms_key_id
    return args


def archive_tables(
    config: BackparqConfig,
    show_stats: bool = False,
    shutdown_event: threading.Event | None = None,
) -> ArchiveResult:
    """
    Archive tables according to configuration.

    Safety model:
    1. Coordinator acquires Advisory Lock (single-process enforcement).
    2. Coordinator calculates Watermarks (MAX(id) before cutoff) for each table.
    3. Workers export data up to the watermark boundary.
    4. Workers delete only rows covered by the watermark (safe from new writes).

    Note: DuckDB opens its own READ COMMITTED connection to PostgreSQL.
    Snapshot isolation is NOT enforced across workers. The watermark is the
    primary safety mechanism for deletion correctness.

    Args:
        config: Backparq configuration
        show_stats: Whether to show statistics at the end
        shutdown_event: Optional event to signal shutdown

    Returns:
        ArchiveResult with statistics and errors
    """
    result = ArchiveResult()
    start_time = time.time()
    shutdown = shutdown_event or threading.Event()

    print_header("BACKPARQ ARCHIVE")
    console.print(f"Mode: [bold]{config.archive.mode}[/bold]")
    console.print(f"Tables: {', '.join(t.name for t in config.archive.tables)}")
    console.print(f"Cutoff: {config.archive.cutoff_exclusive}")
    console.print(f"Dry Run: {'yes' if config.archive.dry_run else 'no'}")
    console.print()

    # Compute cutoff once — all tables use the same point in time
    cutoff = config.archive.cutoff_exclusive or dt.datetime.now(dt.timezone.utc)
    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=dt.timezone.utc)

    # Calculate required connections: (tables * chunks) + buffer + Coordinator
    total_concurrency = config.archive.concurrency * config.archive.chunk_concurrency
    pool = ConnectionPool(config.database, minconn=2, maxconn=total_concurrency + 3)

    # Coordinator Connection & Lock
    try:
        with pool.connection() as coord_conn:
            # 1. Acquire Distributed Lock
            lock = AdvisoryLock(coord_conn, "backparq_archive_lock")
            if not lock.acquire():
                logger.error("Could not acquire advisory lock. Another archive job may be running.")
                result.errors.append("Could not acquire advisory lock")
                return result

            try:
                # 2. Validate tables exist — raise if any are missing
                missing = validate_tables_exist(
                    coord_conn, [t.name for t in config.archive.tables]
                )
                if missing:
                    raise RuntimeError(f"Tables not found in database: {', '.join(missing)}")

                # 3. Calculate Watermarks (MAX(id) at cutoff time per table)
                watermarks: dict[str, Any] = {}
                for t in config.archive.tables:
                    wm_id = pg_get_max_id(
                        coord_conn,
                        t.name,
                        primary_key=t.primary_key,
                        cutoff=cutoff,
                        order_by=t.order_by,
                    )
                    watermarks[t.name] = wm_id
                    logger.info(f"Watermark for {t.name}: {wm_id}")

                # Initialize adapters
                catalog_path = config.archive.base_dir / "backparq.db"
                catalog = Catalog(catalog_path)
                s3 = s3_client_from_config(config.s3) if config.s3.bucket else None

                # Start run tracking
                config_hash = hashlib.sha256(
                    json.dumps(config.__dict__, default=str, sort_keys=True).encode()
                ).hexdigest()[:16]
                run_id = catalog.start_run(config.archive.mode, config_hash)

                # Notify
                if config.notifications:
                    send_notification(
                        config.notifications,
                        "archive_started",
                        {"run_id": run_id, "mode": config.archive.mode},
                    )

                # 4. Process Tables
                with create_progress() as progress:
                    if config.archive.concurrency > 1:
                        logger.info(
                            f"Processing {len(config.archive.tables)} tables with concurrency {config.archive.concurrency}"
                        )
                        with ThreadPoolExecutor(
                            max_workers=config.archive.concurrency
                        ) as executor:
                            futures = {
                                executor.submit(
                                    _process_table,
                                    table_config=table_config,
                                    config=config,
                                    catalog=catalog,
                                    pool=pool,
                                    s3=s3,
                                    run_id=run_id,
                                    cutoff=cutoff,
                                    progress=progress,
                                    result=result,
                                    shutdown=shutdown,
                                    watermark_id=watermarks.get(table_config.name),
                                ): table_config.name
                                for table_config in config.archive.tables
                            }
                            for future in as_completed(futures):
                                if shutdown.is_set():
                                    executor.shutdown(wait=False, cancel_futures=True)
                                    break
                                try:
                                    future.result()
                                except Exception as e:
                                    logger.error(f"Table processing failed: {e}")
                    else:
                        for table_config in config.archive.tables:
                            if shutdown.is_set():
                                break
                            _process_table(
                                table_config=table_config,
                                config=config,
                                catalog=catalog,
                                pool=pool,
                                s3=s3,
                                run_id=run_id,
                                cutoff=cutoff,
                                progress=progress,
                                result=result,
                                shutdown=shutdown,
                                watermark_id=watermarks.get(table_config.name),
                            )

            finally:
                try:
                    lock.release()
                except Exception as e:
                    logger.warning(f"Error releasing lock: {e}")

    except Exception as e:
        logger.error(f"Archive process failed: {e}")
        result.errors.append(str(e))
        pool.close()
        return result

    # Finish run
    elapsed = time.time() - start_time
    if result.errors:
        catalog.finish_run(run_id, RunStatus.FAILED, error="; ".join(result.errors[:3]))
    else:
        catalog.finish_run(run_id, RunStatus.COMPLETED)

    # Notifications
    if config.notifications:
        send_notification(
            config.notifications,
            "archive_success" if not result.errors else "archive_failed",
            {
                "run_id": run_id,
                "elapsed": elapsed,
                "exported": result.total_rows_exported,
                "deleted": result.total_rows_deleted,
                "errors": result.errors,
            },
        )

    if show_stats:
        console.print()
        console.print(f"[bold]Total rows exported:[/bold] {result.total_rows_exported:,}")
        if config.archive.mode == "offload":
            console.print(f"[bold]Total rows deleted:[/bold] {result.total_rows_deleted:,}")
        console.print(f"[bold]Elapsed:[/bold] {elapsed:.1f}s")

    if not result.errors:
        print_success("Archive completed successfully")

    pool.close()
    return result


def _process_table(
    table_config: Any,
    config: BackparqConfig,
    catalog: Catalog,
    pool: ConnectionPool,
    s3: Any,
    run_id: str,
    cutoff: dt.datetime,
    progress: Any,
    result: ArchiveResult,
    shutdown: threading.Event,
    watermark_id: Any,
) -> None:
    """Process a single table."""
    table = table_config.name

    with pool.connection() as conn:
        chunks = list_chunks(
            conn,
            table,
            cutoff,
            order_by=table_config.order_by,
            target_rows=config.archive.chunk_rows,
        )

    if not chunks:
        logger.info(f"No chunks to process for {table}")
        return

    logger.info(f"Processing {table}: {len(chunks)} chunks")

    task = progress.add_task(f"[cyan]{table}", total=len(chunks))

    process_args = {
        "chunks": chunks,
        "table_config": table_config,
        "config": config,
        "catalog": catalog,
        "pool": pool,
        "s3": s3,
        "run_id": run_id,
        "progress": progress,
        "task": task,
        "result": result,
        "shutdown": shutdown,
        "watermark_id": watermark_id,
    }

    if config.archive.chunk_concurrency > 1:
        _process_chunks_parallel(**process_args)
    else:
        _process_chunks_sequential(**process_args)

    with result._lock:
        result.tables_processed += 1


def _process_chunks_sequential(
    chunks: list,
    table_config: Any,
    config: BackparqConfig,
    catalog: Catalog,
    pool: ConnectionPool,
    s3: Any,
    run_id: str,
    progress: Any,
    task: Any,
    result: ArchiveResult,
    shutdown: threading.Event,
    watermark_id: Any,
) -> None:
    """Process chunks sequentially."""
    for chunk in chunks:
        if shutdown.is_set():
            break

        try:
            _process_chunk(
                chunk=chunk,
                table_config=table_config,
                config=config,
                catalog=catalog,
                pool=pool,
                s3=s3,
                run_id=run_id,
                result=result,
                watermark_id=watermark_id,
            )
        except Exception as e:
            logger.error(f"Chunk processing failed: {e}")
            result.errors.append(f"{chunk.table} {chunk.start}: {e}")

        progress.advance(task)


def _process_chunks_parallel(
    chunks: list,
    table_config: Any,
    config: BackparqConfig,
    catalog: Catalog,
    pool: ConnectionPool,
    s3: Any,
    run_id: str,
    progress: Any,
    task: Any,
    result: ArchiveResult,
    shutdown: threading.Event,
    watermark_id: Any,
) -> None:
    """Process chunks in parallel."""
    with ThreadPoolExecutor(max_workers=config.archive.chunk_concurrency) as executor:
        futures = {
            executor.submit(
                _process_chunk,
                chunk=chunk,
                table_config=table_config,
                config=config,
                catalog=catalog,
                pool=pool,
                s3=s3,
                run_id=run_id,
                result=result,
                watermark_id=watermark_id,
            ): chunk
            for chunk in chunks
        }

        for future in as_completed(futures):
            if shutdown.is_set():
                executor.shutdown(wait=False, cancel_futures=True)
                break

            chunk = futures[future]
            try:
                future.result()
            except Exception as e:
                logger.error(f"Chunk processing failed: {e}")
                result.errors.append(f"{chunk.table} {chunk.start}: {e}")

            progress.advance(task)


def _process_chunk(
    chunk: Any,
    table_config: Any,
    config: BackparqConfig,
    catalog: Catalog,
    pool: ConnectionPool,
    s3: Any,
    run_id: str,
    result: ArchiveResult,
    watermark_id: Any,
) -> None:
    """
    Process a single chunk through the pipeline.

    Pipeline stages:
    1. Export: IN_DB → EXPORTED
    2. Upload: EXPORTED → UPLOADED (if S3 configured)
    3. Delete: UPLOADED → OFFLOADED (if offload mode + perform_delete)
    """
    if config.archive.dry_run:
        logger.info(f"[DRY RUN] Would process {chunk}")
        return

    # Stage 1: Export
    with pool.connection() as conn:
        export_stats = export_chunk(
            chunk=chunk,
            conn=conn,
            catalog=catalog,
            base_dir=config.archive.base_dir,
            parquet_config=config.parquet,
            table_config=table_config,
            watermark_id=watermark_id,
            db_config=config.database,
        )

    with result._lock:
        result.total_rows_exported += export_stats.get("rows_exported", 0)

    # Stage 2: Upload (if S3 configured)
    if s3 and config.s3.bucket:
        extra_args = _s3_extra_args(config.s3)

        upload_chunk(
            chunk=chunk,
            catalog=catalog,
            s3_client=s3,
            s3_config=config.s3,
            mode=config.archive.mode,
            run_id=run_id,
            extra_args=extra_args,
        )

    # Stage 3: Delete (if offload mode + perform_delete)
    if (
        config.archive.mode == "offload"
        and config.archive.perform_delete
        and s3
        and config.s3.bucket
    ):
        with pool.connection() as conn:
            delete_stats = delete_chunk(
                chunk=chunk,
                conn=conn,
                catalog=catalog,
                s3_client=s3,
                config=config,
                batch_size=config.archive.delete_batch_size,
            )

        with result._lock:
            result.total_rows_deleted += delete_stats.get("rows_deleted", 0)


def restore_tables(
    config: BackparqConfig,
    start: dt.datetime,
    end: dt.datetime,
    dry_run: bool = False,
    conflict_mode: str = "do_nothing",
    backup_id: str | None = None,
) -> RestoreResult:
    """
    Restore tables from archive.

    Orchestrates the restore pipeline: download → restore.
    """
    result = RestoreResult()
    start_time = time.time()

    print_header("BACKPARQ RESTORE")
    console.print(f"Time Range: {start} to {end}")
    console.print(f"Tables: {', '.join(t.name for t in config.archive.tables)}")
    console.print(f"Conflict Mode: {conflict_mode}")
    console.print(f"Dry Run: {'yes' if dry_run else 'no'}")
    console.print()

    pool = ConnectionPool(config.database, minconn=2, maxconn=config.archive.concurrency + 2)

    try:
        catalog_path = config.archive.base_dir / "backparq.db"
        catalog = Catalog(catalog_path)
        s3 = s3_client_from_config(config.s3)

        with create_progress() as progress:
            for table_config in config.archive.tables:
                table = table_config.name

                all_chunks = catalog.list_chunks(table_name=table)
                chunks_to_restore = []

                for chunk_data in all_chunks:
                    # parse_iso_datetime handles both "+00:00" and "Z" (Python < 3.11 safe)
                    chunk_start = parse_iso_datetime(chunk_data["start_ts"])

                    if not (start <= chunk_start < end):
                        continue

                    state = ChunkState(int(chunk_data["state"]))
                    if state not in (ChunkState.UPLOADED, ChunkState.OFFLOADED):
                        continue

                    chunk_spec = ChunkSpec(
                        table=table,
                        start=parse_iso_datetime(chunk_data["start_ts"]),
                        end=parse_iso_datetime(chunk_data["end_ts"]),
                    )
                    chunks_to_restore.append(chunk_spec)

                if not chunks_to_restore:
                    logger.info(f"No chunks found to restore for {table}")
                    continue

                result.tables_processed += 1
                task = progress.add_task(f"[green]Restoring {table}", total=len(chunks_to_restore))

                def _restore_wrapper(c_spec):
                    with pool.connection() as conn:
                        return restore_chunk(
                            chunk=c_spec,
                            conn=conn,
                            catalog=catalog,
                            s3_client=s3,
                            s3_config=config.s3,
                            conflict_mode=conflict_mode,
                        )

                with ThreadPoolExecutor(max_workers=config.archive.chunk_concurrency) as executor:
                    futures = {
                        executor.submit(_restore_wrapper, chunk): chunk
                        for chunk in chunks_to_restore
                    }

                    for future in as_completed(futures):
                        chunk = futures[future]
                        try:
                            stats = future.result()
                            result.chunks_restored += 1
                            result.rows_restored += stats.get("rows_restored", 0)
                        except Exception as e:
                            logger.error(f"Restore failed for {chunk.table} {chunk.start}: {e}")
                            result.errors.append(f"{chunk.table} {chunk.start}: {e}")

                        progress.advance(task)

    except Exception as e:
        logger.error(f"Restore failed: {e}")
        result.errors.append(str(e))
    finally:
        pool.close()

    result.duration_seconds = time.time() - start_time

    if not result.errors:
        print_success(f"Restore completed: {result.rows_restored:,} rows")

    return result


def verify_archives(
    config: BackparqConfig,
    repair: bool = False,
    table_filter: str | None = None,
) -> VerifyResult:
    """
    Verify archive integrity using catalog and S3.
    """
    result = VerifyResult()

    print_header("BACKPARQ VERIFY")
    console.print(f"Repair Mode: {'yes' if repair else 'no'}")

    try:
        catalog_path = config.archive.base_dir / "backparq.db"
        catalog = Catalog(catalog_path)
        s3 = s3_client_from_config(config.s3) if config.s3.bucket else None

        chunks = catalog.list_chunks(table_name=table_filter)

        with create_progress() as progress:
            task = progress.add_task("Verifying chunks", total=len(chunks))

            with ThreadPoolExecutor(max_workers=config.archive.chunk_concurrency) as executor:
                futures = {
                    executor.submit(
                        verify_chunk,
                        chunk_id=c["id"],
                        catalog=catalog,
                        s3_client=s3,
                        s3_config=config.s3,
                        repair=repair,
                    ): c["id"]
                    for c in chunks
                }

                for future in as_completed(futures):
                    chunk_id = futures[future]
                    try:
                        res = future.result()
                        result.files_checked += 1
                        if res["local_ok"] and res.get("s3_ok", True):
                            result.files_valid += 1
                        else:
                            result.files_corrupted += 1
                            if res.get("repaired"):
                                result.files_repaired += 1
                    except Exception as e:
                        logger.error(f"Verification failed for {chunk_id}: {e}")
                        result.errors.append(f"{chunk_id}: {e}")

                    progress.advance(task)

    except Exception as e:
        result.errors.append(str(e))

    if not result.errors and result.files_corrupted == 0:
        print_success(f"Verification successful: {result.files_valid} chunks valid")

    return result
