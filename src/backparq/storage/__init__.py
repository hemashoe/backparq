"""External storage operations (S3, Parquet)."""

from backparq.storage.s3 import (
    create_client,
    verify_connection,
    upload_file,
    verify_checksum,
    download_file,
    list_objects,
)
from backparq.storage.parquet import (
    safe_mkdir,
    compute_sha256,
    write_manifest,
    load_manifest,
    get_row_count,
    validate_file,
    read_parquet,
    write_parquet,
    build_encryption,
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
    "write_parquet",
    "build_encryption",
]
