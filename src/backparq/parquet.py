import hashlib
import json
from pathlib import Path
from typing import Any, Optional

import pyarrow.parquet as pq

from backparq.config import ParquetConfig


def safe_mkdir(path: Path) -> None:
    path.mkdir(parents=True, exist_ok=True)


def sha256_file(path: Path, buf_size: int = 8 * 1024 * 1024) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            b = f.read(buf_size)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


def write_text(path: Path, text: str) -> None:
    path.write_text(text, encoding="utf-8")


def write_manifest(path: Path, data: dict[str, Any]) -> None:
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(data, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_manifest(path: Path) -> Optional[dict[str, Any]]:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def parquet_footer_rows(path: Path) -> int:
    pf = pq.ParquetFile(path.as_posix())
    return int(pf.metadata.num_rows)


def validate_parquet_file(path: Path, expected_rows: int) -> int:
    if not path.exists():
        raise RuntimeError(f"Parquet file missing: {path}")
    rows = parquet_footer_rows(path)
    if rows != expected_rows:
        raise RuntimeError(
            f"Parquet rowcount mismatch for {path}: expected {expected_rows}, got {rows}."
        )
    metadata = pq.read_metadata(path.as_posix())
    if int(metadata.num_rows) != expected_rows:
        raise RuntimeError(
            f"Parquet metadata mismatch for {path}: expected {expected_rows}, got {metadata.num_rows}."
        )
    row_group_rows = sum(metadata.row_group(i).num_rows for i in range(metadata.num_row_groups))
    if row_group_rows != expected_rows:
        raise RuntimeError(
            f"Parquet row group mismatch for {path}: expected {expected_rows}, got {row_group_rows}."
        )
    if metadata.num_row_groups:
        pq.ParquetFile(path.as_posix()).read_row_group(0)
    return rows


def build_encryption_properties(config: ParquetConfig):
    if not config.encryption.enabled:
        return None

    try:
        encryption_config_class = getattr(pq, "EncryptionConfiguration", None)
        if encryption_config_class is None:
            encryption_config_class = getattr(pq, "ParquetEncryptionConfiguration", None)
        crypto_factory_class = getattr(pq, "CryptoFactory", None)
        kms_connection_class = getattr(pq, "KmsConnectionConfig", None)
    except Exception as exc:
        raise RuntimeError("PyArrow encryption configuration could not be loaded.") from exc

    if not (encryption_config_class and crypto_factory_class and kms_connection_class):
        raise RuntimeError("PyArrow build does not support Parquet encryption.")

    if not config.encryption.key_map:
        raise RuntimeError("Parquet encryption requires 'key_map' entries.")

    if not config.encryption.footer_key:
        raise RuntimeError("Parquet encryption requires 'footer_key'.")

    kms_config = kms_connection_class(custom_kms_conf=config.encryption.key_map)
    crypto_factory = crypto_factory_class(config.encryption.key_map, kms_config)
    encryption_config = encryption_config_class(
        footer_key=config.encryption.footer_key,
        column_keys=config.encryption.column_keys,
    )
    return crypto_factory.file_encryption_properties(encryption_config)


def read_chunk(path: Path, encryption_properties=None):
    """
    Reads a Parquet file into a PyArrow Table.
    """
    import pyarrow.parquet as pq
    
    # If encryption properties are provided, we need to use a specialized ParquetDataset or direct ParquetFile read
    # pq.read_table automatically handles decryption if properties are set?
    # Actually, usually you need to pass decryption_properties.
    
    # For now, let's assume we reuse the same properties for both read and write if symmetric,
    # OR we need to build DecryptionProperties.
    # But usually CryptoFactory handles it if we use it correctly.
    
    decryption_properties = None
    if encryption_properties:
        # This is a bit complex. The provided object is FileEncryptionProperties.
        # We need FileDecryptionProperties.
        # In this simple implementation, we might not support reading encrypted files fully yet 
        # unless we parse the config again for Decryption.
        # Let's assume standard read_table works if keys are in keyring or handled by environment?
        # No, we need explicit decryption setup.
        pass

    # Basic read for now. 
    # TODO: Proper decryption support requires rebuilding CryptoFactory for decryption.
    # For the MVP, if encryption is enabled, we assume the user has configured the environment/keys such that
    # simple read might fail without properties.
    
    try:
        return pq.read_table(path.as_posix(), decryption_properties=decryption_properties)
    except Exception as exc:
        raise RuntimeError(f"Failed to read parquet file {path}: {exc}") from exc
