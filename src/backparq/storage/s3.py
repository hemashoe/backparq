"""S3 storage operations."""

from __future__ import annotations

import logging

import boto3
import botocore
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import S3Config

logger = logging.getLogger(__name__)


def create_client(config: S3Config):
    """Create S3 client from configuration."""
    s3_config = None
    if config.addressing_style:
        s3_config = Config(
            s3={"addressing_style": config.addressing_style},
            max_pool_connections=50,
        )
    else:
        s3_config = Config(max_pool_connections=50)

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
    s3,
    path,
    bucket: str,
    key: str,
    sha256: str,
    extra_args: dict = None,
    metadata: dict = None,
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


def verify_checksum(s3, bucket: str, key: str, expected: str) -> bool:
    """Check if S3 object exists with matching SHA256."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        actual = head.get("Metadata", {}).get("sha256", "")
        if actual == expected:
            return True
        logger.warning(f"Checksum mismatch for {key}")
        return False
    except botocore.exceptions.ClientError:
        return False


def download_file(s3, bucket: str, key: str, path: str) -> None:
    """Download file from S3."""
    s3.download_file(bucket, key, path)


def list_objects(s3, bucket: str, prefix: str) -> list:
    """List objects under prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    objects = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            objects.append(obj)
    return objects
