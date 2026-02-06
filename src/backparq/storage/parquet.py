"""Parquet file operations."""

from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Optional

import pyarrow.parquet as pq

from backparq.config import ParquetConfig

logger = logging.getLogger(__name__)


def safe_mkdir(path: Path) -> None:
    """Create directory and parents."""
    path.mkdir(parents=True, exist_ok=True)


def compute_sha256(path: Path, buf_size: int = 8 * 1024 * 1024) -> str:
    """Compute SHA256 hash of file."""
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(buf_size):
            h.update(chunk)
    return h.hexdigest()


def write_manifest(path: Path, data: dict) -> None:
    """Write manifest JSON atomically."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_manifest(path: Path) -> Optional[dict]:
    """Load manifest if exists."""
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None


def get_row_count(path: Path) -> int:
    """Read row count from Parquet footer."""
    return pq.ParquetFile(str(path)).metadata.num_rows


def validate_file(path: Path, expected_rows: int) -> int:
    """Validate Parquet file. Returns row count."""
    if not path.exists():
        raise RuntimeError(f"File not found: {path}")
    rows = get_row_count(path)
    if rows != expected_rows:
        raise RuntimeError(f"Row count mismatch: expected {expected_rows}, got {rows}")
    return rows


def read_parquet(path: Path, decryption_props=None):
    """Read Parquet file to Arrow table (loads entire file into memory)."""
    return pq.read_table(str(path), decryption_properties=decryption_props)


def read_parquet_batches(path: Path, batch_size: int = 10_000, decryption_props=None):
    """
    Stream Parquet file as batches to handle large files without memory issues.

    Yields Arrow RecordBatches that can be processed incrementally.
    This is preferred for restore operations on large files.

    Args:
        path: Path to the Parquet file
        batch_size: Number of rows per batch (default 10,000)
        decryption_props: Optional decryption properties

    Yields:
        pyarrow.RecordBatch objects
    """
    parquet_file = pq.ParquetFile(str(path), decryption_properties=decryption_props)
    yield from parquet_file.iter_batches(batch_size=batch_size)


def get_parquet_schema(path: Path, decryption_props=None):
    """Get schema from Parquet file without reading all data."""
    parquet_file = pq.ParquetFile(str(path), decryption_properties=decryption_props)
    return parquet_file.schema_arrow


def write_parquet(table, path: Path, compression: str = "snappy", encryption_props=None) -> None:
    """Write Arrow table to Parquet."""
    pq.write_table(
        table, str(path), compression=compression, encryption_properties=encryption_props
    )


def build_encryption(config: ParquetConfig):
    """Build encryption properties. Returns None if disabled."""
    if not config.encryption.enabled:
        return None

    enc_cfg_cls = getattr(pq, "EncryptionConfiguration", None) or getattr(
        pq, "ParquetEncryptionConfiguration", None
    )
    crypto_cls = getattr(pq, "CryptoFactory", None)
    kms_cls = getattr(pq, "KmsConnectionConfig", None)

    if not (enc_cfg_cls and crypto_cls and kms_cls):
        raise RuntimeError("PyArrow build does not support Parquet encryption")

    if not config.encryption.key_map or not config.encryption.footer_key:
        raise RuntimeError("Encryption requires footer_key and key_map")

    kms_config = kms_cls(custom_kms_conf=config.encryption.key_map)
    crypto_factory = crypto_cls(config.encryption.key_map, kms_config)
    enc_config = enc_cfg_cls(
        footer_key=config.encryption.footer_key,
        column_keys=config.encryption.column_keys,
    )
    return crypto_factory.file_encryption_properties(enc_config)
