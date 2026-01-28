"""Parquet file operations for backparq."""

import hashlib
import json
import logging
from pathlib import Path
from typing import Any, Optional

import pyarrow.parquet as pq

from backparq.config import ParquetConfig

logger = logging.getLogger(__name__)


def safe_mkdir(path: Path) -> None:
    """Create directory and parents if needed."""
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, buf_size: int = 8 * 1024 * 1024) -> str:
    """Compute SHA256 hash of file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(buf_size):
            h.update(chunk)
    return h.hexdigest()


def write_text(path: Path, text: str) -> None:
    """Write text to file."""
    path.write_text(text, encoding="utf-8")


def write_manifest(path: Path, data: dict[str, Any]) -> None:
    """Write manifest JSON atomically."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_manifest(path: Path) -> Optional[dict[str, Any]]:
    """Load manifest JSON if exists."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def parquet_footer_rows(path: Path) -> int:
    """Read row count from Parquet footer."""
    return int(pq.ParquetFile(path.as_posix()).metadata.num_rows)


def validate_parquet_file(path: Path, expected_rows: int) -> int:
    """Validate Parquet file integrity. Returns row count."""
    if not path.exists():
        raise RuntimeError(f"Parquet file missing: {path}")

    rows = parquet_footer_rows(path)
    if rows != expected_rows:
        raise RuntimeError(f"Row count mismatch: expected {expected_rows}, got {rows}")

    metadata = pq.read_metadata(path.as_posix())
    if metadata.num_row_groups:
        pq.ParquetFile(path.as_posix()).read_row_group(0)

    return rows


def build_encryption_properties(config: ParquetConfig):
    """Build Parquet encryption properties. Returns None if disabled."""
    if not config.encryption.enabled:
        return None

    encryption_config_class = getattr(pq, "EncryptionConfiguration", None) or getattr(
        pq, "ParquetEncryptionConfiguration", None
    )
    crypto_factory_class = getattr(pq, "CryptoFactory", None)
    kms_connection_class = getattr(pq, "KmsConnectionConfig", None)

    if not (encryption_config_class and crypto_factory_class and kms_connection_class):
        raise RuntimeError("PyArrow build does not support Parquet encryption")

    if not config.encryption.key_map or not config.encryption.footer_key:
        raise RuntimeError("Encryption requires footer_key and key_map")

    kms_config = kms_connection_class(custom_kms_conf=config.encryption.key_map)
    crypto_factory = crypto_factory_class(config.encryption.key_map, kms_config)
    encryption_config = encryption_config_class(
        footer_key=config.encryption.footer_key,
        column_keys=config.encryption.column_keys,
    )
    return crypto_factory.file_encryption_properties(encryption_config)


def read_chunk(path: Path, decryption_properties=None):
    """Read Parquet file into PyArrow Table."""
    try:
        return pq.read_table(path.as_posix(), decryption_properties=decryption_properties)
    except Exception as exc:
        raise RuntimeError(f"Failed to read {path}: {exc}") from exc
