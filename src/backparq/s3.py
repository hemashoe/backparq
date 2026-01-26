from typing import Optional

import boto3
import botocore
from botocore.config import Config

from backparq.config import S3Config


from tenacity import retry,  stop_after_attempt, wait_exponential

def s3_client_from_config(config: S3Config):
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


def test_s3_connection(config: S3Config) -> None:
    s3 = s3_client_from_config(config)
    try:
        s3.head_bucket(Bucket=config.bucket)
    except botocore.exceptions.ClientError as exc:
        raise RuntimeError(f"S3 bucket not reachable: {config.bucket}") from exc


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=60))
def s3_upload_file(
    s3,
    local_path,
    bucket: str,
    key: str,
    sha256_hex: str,
    extra_args: Optional[dict],
) -> None:
    args = {"Metadata": {"sha256": sha256_hex}}
    if extra_args:
        args.update(extra_args)
    s3.upload_file(local_path.as_posix(), bucket, key, ExtraArgs=args)


def s3_verify_object_sha256(s3, bucket: str, key: str, expected_sha256: str) -> bool:
    try:
        head = s3.head_object(Bucket=bucket, Key=key)
    except botocore.exceptions.ClientError:
        return False
    meta = head.get("Metadata", {}) or {}
    return meta.get("sha256") == expected_sha256
