from __future__ import annotations

import logging
from pathlib import Path
from typing import Optional

from backparq.config import BackparqConfig
from backparq.models import VerifyResult
from backparq.storage.parquet import compute_sha256, load_manifest
from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.storage.s3 import verify_checksum as s3_verify_object_sha256
from backparq.utils.console import (
    console,
    create_progress,
    print_error,
    print_header,
    print_success,
    print_warning,
)

logger = logging.getLogger(__name__)


def verify_archives(
    config: BackparqConfig,
    repair: bool = False,
    table_filter: Optional[str] = None,
) -> VerifyResult:
    result = VerifyResult()

    print_header("BACKPARQ VERIFY")
    console.print(f"Base Dir: {config.archive.base_dir}")
    console.print(
        f"S3: s3://{config.s3.bucket}/{config.s3.prefix}"
        if config.s3.bucket
        else "S3: Not configured"
    )
    console.print(f"Repair: {'enabled' if repair else 'disabled'}")
    console.print()

    s3 = None
    if config.s3.bucket:
        try:
            s3 = s3_client_from_config(config.s3)
        except Exception as e:
            logger.warning(f"S3 connection failed: {e}")

    parquet_dir = config.archive.base_dir / "parquet"
    if not parquet_dir.exists():
        print_warning("No local parquet files found")
        return result

    parquet_files = list(parquet_dir.rglob("*.parquet"))
    parquet_files = [f for f in parquet_files if not f.name.endswith(".inprogress")]

    if table_filter:
        safe_filter = table_filter.replace(".", "_")
        parquet_files = [f for f in parquet_files if safe_filter in str(f)]

    if not parquet_files:
        print_warning("No parquet files to verify")
        return result

    console.print(f"Found {len(parquet_files)} parquet files")
    console.print()

    with create_progress() as progress:
        task = progress.add_task("Verifying", total=len(parquet_files))

        for parquet_path in parquet_files:
            result.files_checked += 1

            manifest_path = parquet_path.with_suffix(".parquet.manifest.json")
            manifest = load_manifest(manifest_path)

            if not manifest:
                result.files_corrupted += 1
                result.errors.append(f"Missing manifest: {parquet_path.name}")
                progress.advance(task)
                continue

            expected_sha = manifest.get("sha256", "")
            if expected_sha:
                actual_sha = compute_sha256(parquet_path)
                if actual_sha != expected_sha:
                    result.files_corrupted += 1
                    result.errors.append(f"Checksum mismatch: {parquet_path.name}")
                    if repair and s3:
                        if _repair_from_s3(s3, config, parquet_path, manifest):
                            result.files_repaired += 1
                    progress.advance(task)
                    continue

            if s3 and config.s3.bucket:
                s3_key = manifest.get("s3_key", "")
                if s3_key and not s3_verify_object_sha256(
                    s3, config.s3.bucket, s3_key, expected_sha
                ):
                    result.errors.append(f"S3 mismatch: {s3_key}")
                    if repair:
                        if _repair_to_s3(s3, config, parquet_path, s3_key, expected_sha):
                            result.files_repaired += 1

            result.files_valid += 1
            progress.advance(task)

    console.print()
    if result.success:
        print_success(f"Verified {result.files_valid}/{result.files_checked} files")
    else:
        print_error(f"Found {result.files_corrupted} corrupted files")
        if result.files_repaired:
            print_success(f"Repaired {result.files_repaired} files")

    return result


def _repair_from_s3(s3, config: BackparqConfig, local_path: Path, manifest: dict) -> bool:
    s3_key = manifest.get("s3_key", "")
    if not s3_key:
        return False
    try:
        s3.download_file(config.s3.bucket, s3_key, str(local_path))
        return True
    except Exception as e:
        logger.error(f"Repair failed: {e}")
        return False


def _repair_to_s3(
    s3, config: BackparqConfig, local_path: Path, s3_key: str, expected_sha: str
) -> bool:
    try:
        s3.upload_file(
            str(local_path),
            config.s3.bucket,
            s3_key,
            ExtraArgs={"Metadata": {"sha256": expected_sha}},
        )
        return True
    except Exception as e:
        logger.error(f"Upload failed: {e}")
        return False
