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


def s3_key_for_chunk(prefix: str, chunk: ChunkSpec) -> str:
    year = chunk.start.year
    month = chunk.start.month
    prefix = prefix.rstrip("/")
    return (
        f"{prefix}/{chunk.table}/year={year:04d}/month={month:02d}/"
        f"{chunk.table}_{year:04d}-{month:02d}.parquet"
    )


def _s3_extra_args(config) -> dict:
    extra_args: dict = {}
    if config.sse:
        extra_args["ServerSideEncryption"] = config.sse
    if config.sse == "aws:kms" and config.kms_key_id:
        extra_args["SSEKMSKeyId"] = config.kms_key_id
    return extra_args


def archive_tables(config: BackparqConfig) -> None:
    ensure_under_base_dir(config.archive.base_dir)

    do_upload = bool(config.s3.bucket)
    s3 = None
    extra_args = _s3_extra_args(config.s3)

    if config.archive.dry_run:
        print("DRY RUN: will not write/upload/delete.\n")

    encryption_properties = build_encryption_properties(config.parquet)

    test_pg_connection(config.database)
    if do_upload:
        test_s3_connection(config.s3)

    if do_upload and not config.archive.dry_run:
        s3 = s3_client_from_config(config.s3)

    conn = connect_pg(config.database)
    try:
        for table in config.archive.tables:
            print(f"\n=== TABLE: {table} ===")
            chunks = list_chunks(conn, table, config.archive.cutoff_exclusive)
            if not chunks:
                print("No data found (or table empty).")
                continue

            for chunk in chunks:
                year, month = chunk.start.year, chunk.start.month
                final_parquet, inprogress_parquet, sha_path, manifest_path = chunk_paths(
                    config.archive.base_dir, chunk
                )
                s3_key = s3_key_for_chunk(config.s3.prefix, chunk)

                print(
                    f"\n-- Chunk {year:04d}-{month:02d}: "
                    f"[{chunk.start.isoformat()} .. {chunk.end.isoformat()})"
                )
                db_rows = pg_count_rows(conn, table, chunk.start, chunk.end)
                print(f"DB rows in chunk: {db_rows}")

                if config.archive.dry_run:
                    if db_rows:
                        print(f"PLAN: export -> {final_parquet}")
                        if do_upload:
                            print(f"PLAN: upload -> s3://{config.s3.bucket}/{s3_key}")
                        if config.archive.perform_delete:
                            print("PLAN: delete after verified archive+upload")
                    else:
                        print("PLAN: skip empty chunk")
                    continue

                if db_rows == 0:
                    print("Skipping empty chunk.")
                    continue

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
                        print("Already archived & verified enough for export step. Skipping export/upload.")
                        continue

                    if config.archive.perform_delete and already_verified:
                        print(
                            "Chunk already archived & verified. "
                            "Proceeding directly to deletion (delete-later mode)."
                        )

                if not (config.archive.perform_delete and already_verified) and not already_archived:
                    if inprogress_parquet.exists():
                        inprogress_parquet.unlink()

                    if final_parquet.exists() and not config.archive.overwrite:
                        raise RuntimeError(
                            f"{final_parquet} exists but no valid manifest or mismatch. "
                            "Use overwrite or delete the file."
                        )

                    if final_parquet.exists() and config.archive.overwrite:
                        final_parquet.unlink()
                    if sha_path.exists() and config.archive.overwrite:
                        sha_path.unlink()
                    if manifest_path.exists() and config.archive.overwrite:
                        manifest_path.unlink()

                    print(f"Exporting to Parquet (atomic): {inprogress_parquet} -> {final_parquet}")
                    t0 = time.time()
                    exported = export_chunk_to_parquet_streaming(
                        conn=conn,
                        table=table,
                        start=chunk.start,
                        end=chunk.end,
                        parquet_path=inprogress_parquet,
                        order_by=config.archive.order_by,
                        fetch_size=config.archive.fetch_size,
                        compression=config.parquet.compression,
                        encryption_properties=encryption_properties,
                    )
                    print(f"Exported rows: {exported} in {time.time() - t0:.1f}s")

                    if exported != db_rows:
                        raise RuntimeError(
                            f"Exported rows ({exported}) != DB count ({db_rows}). Refusing."
                        )

                    validate_parquet_file(inprogress_parquet, exported)
                    inprogress_parquet.replace(final_parquet)

                    validate_parquet_file(final_parquet, exported)

                    sha = sha256_file(final_parquet)
                    write_text(sha_path, sha + "\n")
                    print(f"SHA256: {sha}")

                    uploaded_and_verified = False
                    if do_upload:
                        if s3 is None:
                            raise RuntimeError("S3 client not initialized.")
                        print(f"Uploading to s3://{config.s3.bucket}/{s3_key}")
                        s3_upload_file(
                            s3=s3,
                            local_path=final_parquet,
                            bucket=config.s3.bucket,
                            key=s3_key,
                            sha256_hex=sha,
                            extra_args=extra_args or None,
                        )
                        uploaded_and_verified = s3_verify_object_sha256(
                            s3, config.s3.bucket, s3_key, sha
                        )
                        if not uploaded_and_verified:
                            raise RuntimeError(
                                "S3 upload verification failed (sha256 metadata mismatch)."
                            )
                        print("S3 upload verified (sha256 metadata matches).")
                    else:
                        print("S3 upload skipped (no S3 bucket set).")

                    manifest = {
                        "table": table,
                        "chunk_start": chunk.start.isoformat(),
                        "chunk_end": chunk.end.isoformat(),
                        "cutoff_exclusive": config.archive.cutoff_exclusive.isoformat(),
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
                    print(f"Manifest written: {manifest_path}")

                if config.archive.perform_delete:
                    manifest = load_manifest(manifest_path)
                    if not manifest:
                        raise RuntimeError("perform_delete set but no manifest found.")

                    exported_rows = int(manifest.get("exported_rows", -1))
                    validate_parquet_file(final_parquet, exported_rows)

                    if do_upload:
                        if s3 is None:
                            s3 = s3_client_from_config(config.s3)
                        mb = manifest.get("s3_bucket", config.s3.bucket)
                        mk = manifest.get("s3_key", s3_key)
                        sha = manifest.get("sha256", "")
                        if not sha or not s3_verify_object_sha256(s3, mb, mk, sha):
                            raise RuntimeError("S3 verification failed/not present. Refusing delete.")

                    db_rows_now = pg_count_rows(conn, table, chunk.start, chunk.end)
                    if db_rows_now != exported_rows:
                        raise RuntimeError(
                            "DB rowcount changed since export "
                            f"(now {db_rows_now}, exported {exported_rows})."
                        )

                    print(
                        f"Deleting archived rows from {table} for {year:04d}-{month:02d} "
                        f"in batches of {config.archive.delete_batch_size} ..."
                    )
                    deleted = delete_chunk_safely(
                        conn=conn,
                        table=table,
                        start=chunk.start,
                        end=chunk.end,
                        batch_size=config.archive.delete_batch_size,
                    )
                    print(f"Deleted rows: {deleted}")

                    if deleted != exported_rows:
                        print(
                            f"WARNING: deleted ({deleted}) != exported ({exported_rows}). "
                            "Investigate immediately."
                        )
                    else:
                        print("Delete completed and counts match.")

        print("\nAll done.")
    finally:
        conn.close()
