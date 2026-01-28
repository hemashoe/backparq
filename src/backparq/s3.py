"""S3 operations for backparq."""

import logging
from typing import Optional

import boto3
import botocore
from botocore.config import Config
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import S3Config

logger = logging.getLogger(__name__)


def s3_client_from_config(config: S3Config):
    """Create an S3 client from configuration."""
    s3_config = None
    if config.addressing_style:
        s3_config = Config(s3={"addressing_style": config.addressing_style})

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


def verify_s3_connection(config: S3Config) -> None:
    """Test S3 bucket connectivity."""
    logger.info(f"Testing S3 connection to bucket: {config.bucket}")
    s3 = s3_client_from_config(config)
    try:
        s3.head_bucket(Bucket=config.bucket)
        logger.info("S3 connection successful")
    except botocore.exceptions.ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        logger.error(f"S3 bucket not accessible: {config.bucket} ({error_code})")
        raise RuntimeError(f"S3 bucket not accessible: {config.bucket}") from exc


# Backward compatibility alias
test_s3_connection = verify_s3_connection


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
def s3_upload_file(
    s3, local_path, bucket: str, key: str, sha256_hex: str, extra_args: Optional[dict]
) -> None:
    """Upload file to S3 with retry and SHA256 metadata."""
    args = {"Metadata": {"sha256": sha256_hex}}
    if extra_args:
        args.update(extra_args)
    s3.upload_file(local_path.as_posix(), bucket, key, ExtraArgs=args)


def s3_verify_object_sha256(s3, bucket: str, key: str, expected_sha256: str) -> bool:
    """Verify S3 object exists and has matching SHA256."""
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
        actual_sha = head.get("Metadata", {}).get("sha256", "")
        if actual_sha == expected_sha256:
            return True
        logger.warning(f"Checksum mismatch for {key}")
        return False
    except botocore.exceptions.ClientError:
        return False


def s3_download_file(s3, bucket: str, key: str, local_path: str) -> None:
    """Download file from S3."""
    s3.download_file(bucket, key, local_path)
