"""External storage operations (S3, Parquet)."""

from backparq.storage.parquet import (
    build_encryption,
    compute_sha256,
    get_parquet_schema,
    get_row_count,
    load_manifest,
    read_parquet,
    read_parquet_batches,
    safe_mkdir,
    validate_file,
    write_manifest,
    write_parquet,
)
from backparq.storage.s3 import (
    create_client,
    download_file,
    list_objects,
    upload_file,
    verify_checksum,
    verify_connection,
)

__all__ = [
    "create_client",
    "verify_connection",
    "upload_file",
    "verify_checksum",
    "download_file",
    "list_objects",
    "safe_mkdir",
    "compute_sha256",
    "write_manifest",
    "load_manifest",
    "get_row_count",
    "validate_file",
    "read_parquet",
    "read_parquet_batches",
    "get_parquet_schema",
    "write_parquet",
    "build_encryption",
]
