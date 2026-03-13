"""S3 storage operations."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import boto3
import botocore
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import S3Config

logger = logging.getLogger(__name__)

# Default timeouts in seconds
DEFAULT_CONNECT_TIMEOUT = 30
DEFAULT_READ_TIMEOUT = 60


def create_client(config: S3Config) -> Any:
    """Create S3 client from configuration with proper timeouts."""
    s3_config_kwargs = {
        "max_pool_connections": 50,
        "connect_timeout": DEFAULT_CONNECT_TIMEOUT,
        "read_timeout": DEFAULT_READ_TIMEOUT,
        "retries": {
            "max_attempts": 3,
            "mode": "adaptive",
        },
    }

    if config.addressing_style:
        s3_config_kwargs["s3"] = {"addressing_style": config.addressing_style}

    s3_config = Config(**s3_config_kwargs)

    session = boto3.session.Session(
        aws_access_key_id=config.access_key_id,
        aws_secret_access_key=config.secret_access_key,
        aws_session_token=config.session_token,
        region_name=config.region,
    )

    return session.client(
        "s3",
        endpoint_url=config.endpoint_url,
        use_ssl=config.use_ssl,
        verify=config.verify_ssl,
        config=s3_config,
    )


def verify_connection(config: S3Config) -> None:
    """Test S3 bucket connectivity."""
    s3 = create_client(config)
    try:
        s3.head_bucket(Bucket=config.bucket)
        logger.info(f"S3 connection OK: {config.bucket}")
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "Unknown")
        raise RuntimeError(f"S3 bucket not accessible: {config.bucket} ({code})") from exc


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
def upload_file(
    s3: Any,
    path: Path,
    bucket: str,
    key: str,
    sha256: str,
    extra_args: Optional[dict[str, Any]] = None,
    metadata: Optional[dict[str, Any]] = None,
) -> None:
    """Upload file to S3 with SHA256 and custom metadata."""
    meta = {"sha256": sha256}
    if metadata:
        # Convert all values to string for S3 metadata
        meta.update({k: str(v) for k, v in metadata.items()})

    args = {"Metadata": meta}
    if extra_args:
        args.update(extra_args)
    s3.upload_file(str(path), bucket, key, ExtraArgs=args)
    logger.debug(f"Uploaded {key}")


def verify_checksum(s3: Any, bucket: str, key: str, expected: str) -> bool:
    """
    Check if S3 object exists with matching SHA256.

    Returns False for 404 (object missing).
    Re-raises for all other errors (auth failure, network, etc.) so callers
    cannot silently mistake a transient error for a checksum mismatch.
    """
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        actual = head.get("Metadata", {}).get("sha256", "")
        if actual == expected:
            return True
        logger.warning(f"Checksum mismatch for {key}: expected {expected!r}, got {actual!r}")
        return False
    except botocore.exceptions.ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        if code in ("404", "NoSuchKey"):
            logger.warning(f"S3 object not found: {key}")
            return False
        # Unexpected error (403 Forbidden, network, etc.) — propagate it.
        raise


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
def download_file(s3: Any, bucket: str, key: str, path: str) -> None:
    """Download file from S3 with retry on transient failures."""
    s3.download_file(bucket, key, path)


def list_objects(s3: Any, bucket: str, prefix: str) -> list[Any]:
    """List objects under prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(obj)
    return objects
