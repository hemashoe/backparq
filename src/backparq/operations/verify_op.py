"""Verify operation: Checks SHA256 of local file and S3 object.

Optionally repairs corrupted files.
"""

from __future__ import annotations

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from backparq.adapters.catalog import Catalog
    from backparq.config import S3Config

from backparq.primitives import compute_sha256

logger = logging.getLogger(__name__)


def verify_chunk(
    chunk_id: str,
    catalog: Catalog,
    s3_client: Any | None = None,
    s3_config: S3Config | None = None,
    repair: bool = False,
) -> dict[str, Any]:
    """
    Verify chunk integrity.

    Args:
        chunk_id: Chunk identifier
        catalog: Catalog for state tracking
        s3_client: Optional boto3 S3 client for S3 verification
        s3_config: Optional S3 configuration
        repair: If True, attempt to repair corrupted files

    Returns:
        Dict with verification results: local_ok, s3_ok, repaired

    Raises:
        Exception: If verification fails and repair is not enabled
    """
    from backparq.storage.s3 import download_file, upload_file, verify_checksum

    # Get chunk data
    chunk_data = catalog.get_chunk(chunk_id)
    if not chunk_data:
        raise ValueError(f"Chunk {chunk_id} not found in catalog")

    expected_sha256 = chunk_data["sha256"]
    local_path_str = chunk_data.get("local_path")
    s3_key = chunk_data.get("s3_key")

    results = {"local_ok": None, "s3_ok": None, "repaired": False}

    # Verify local file
    if local_path_str:
        local_path = Path(local_path_str)
        if local_path.exists():
            actual_sha256 = compute_sha256(local_path)
            results["local_ok"] = actual_sha256 == expected_sha256

            if not results["local_ok"]:
                logger.warning(f"Local file checksum mismatch for {chunk_id}")
                if repair and s3_client and s3_config and s3_key:
                    # Re-download from S3
                    logger.info(f"Repairing local file from S3: {chunk_id}")
                    download_file(s3_client, s3_config.bucket, s3_key, local_path)
                    actual_sha256 = compute_sha256(local_path)
                    results["local_ok"] = actual_sha256 == expected_sha256
                    results["repaired"] = results["local_ok"]
        else:
            results["local_ok"] = False
            logger.warning(f"Local file not found: {local_path}")

    # Verify S3 object
    if s3_client and s3_config and s3_key:
        results["s3_ok"] = verify_checksum(s3_client, s3_config.bucket, s3_key, expected_sha256)

        if not results["s3_ok"]:
            logger.warning(f"S3 object checksum mismatch for {chunk_id}")
            if repair and local_path_str:
                local_path = Path(local_path_str)
                if local_path.exists() and results["local_ok"]:
                    # Re-upload from local
                    logger.info(f"Repairing S3 object from local file: {chunk_id}")
                    upload_file(
                        s3=s3_client,
                        path=local_path,
                        bucket=s3_config.bucket,
                        key=s3_key,
                        sha256=expected_sha256,
                    )
                    results["s3_ok"] = verify_checksum(
                        s3_client, s3_config.bucket, s3_key, expected_sha256
                    )
                    results["repaired"] = results["s3_ok"]

    return results
