import dataclasses
import datetime as dt
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import yaml

REQUIRED_BASE_DIR = Path("/mnt/HC_Volume_101950313/db-archive-data").resolve()
DEFAULT_CUTOFF_EXCLUSIVE = "2025-08-01"
DEFAULT_S3_PREFIX = "db-archive"
DEFAULT_ORDER_BY = "created_at"
DEFAULT_COMPRESSION = "snappy"


@dataclass(frozen=True)
class DatabaseConfig:
    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str = ""
    connect_timeout: int = 10

    def dsn(self) -> str:
        parts = [
            f"host={self.host}",
            f"port={self.port}",
            f"dbname={self.name}",
            f"user={self.user}",
            f"password={self.password}",
            f"connect_timeout={self.connect_timeout}",
        ]
        if self.sslmode:
            parts.append(f"sslmode={self.sslmode}")
        return " ".join(parts)


@dataclass(frozen=True)
class S3Config:
    bucket: str
    prefix: str = DEFAULT_S3_PREFIX
    region: Optional[str] = None
    access_key_id: Optional[str] = None
    secret_access_key: Optional[str] = None
    session_token: Optional[str] = None
    endpoint_url: Optional[str] = None
    use_ssl: bool = True
    verify_ssl: bool = True
    addressing_style: str = ""
    sse: str = ""
    kms_key_id: str = ""


@dataclass(frozen=True)
class ParquetEncryptionConfig:
    enabled: bool = False
    footer_key: str = ""
    column_keys: dict[str, str] = dataclasses.field(default_factory=dict)
    key_map: dict[str, str] = dataclasses.field(default_factory=dict)


@dataclass(frozen=True)
class ParquetConfig:
    compression: str = DEFAULT_COMPRESSION
    encryption: ParquetEncryptionConfig = dataclasses.field(
        default_factory=ParquetEncryptionConfig
    )


@dataclass(frozen=True)
class ArchiveConfig:
    tables: list[str]
    cutoff_exclusive: dt.datetime
    base_dir: Path
    fetch_size: int = 10_000
    overwrite: bool = False
    dry_run: bool = False
    perform_delete: bool = False
    delete_batch_size: int = 10_000
    order_by: str = DEFAULT_ORDER_BY


@dataclass(frozen=True)
class CronConfig:
    enabled: bool = False
    schedule: str = ""
    command: str = ""


@dataclass(frozen=True)
class BackparqConfig:
    database: DatabaseConfig
    s3: S3Config
    parquet: ParquetConfig
    archive: ArchiveConfig
    cron: CronConfig = dataclasses.field(default_factory=CronConfig)


class ConfigError(ValueError):
    pass


def parse_utc_datetime(value: str) -> dt.datetime:
    if "T" in value:
        parsed = dt.datetime.fromisoformat(value)
    else:
        parsed = dt.datetime.fromisoformat(value + "T00:00:00")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def ensure_under_base_dir(path: Path) -> None:
    resolved = path.resolve()
    if REQUIRED_BASE_DIR not in resolved.parents and resolved != REQUIRED_BASE_DIR:
        raise ConfigError(f"Refusing to write outside {REQUIRED_BASE_DIR}. Got: {resolved}")


def _require_section(data: dict[str, Any], key: str) -> dict[str, Any]:
    value = data.get(key)
    if value is None or not isinstance(value, dict):
        raise ConfigError(f"Missing or invalid '{key}' section in config.")
    return value


def _require_str(data: dict[str, Any], key: str) -> str:
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Missing or invalid '{key}' value in config.")
    return value.strip()


def _optional_str(data: dict[str, Any], key: str, default: str = "") -> str:
    value = data.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"Invalid '{key}' value in config.")
    return value.strip()


def _optional_int(data: dict[str, Any], key: str, default: int) -> int:
    value = data.get(key, default)
    if isinstance(value, bool):
        raise ConfigError(f"Invalid '{key}' value in config (bool not allowed).")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Invalid '{key}' value in config.") from exc


def _optional_bool(data: dict[str, Any], key: str, default: bool = False) -> bool:
    value = data.get(key, default)
    if isinstance(value, bool):
        return value
    raise ConfigError(f"Invalid '{key}' value in config; must be boolean.")


def _optional_str_map(data: dict[str, Any], key: str) -> dict[str, str]:
    value = data.get(key, {})
    if value is None:
        return {}
    if not isinstance(value, dict):
        raise ConfigError(f"Invalid '{key}' value in config; must be mapping.")
    parsed: dict[str, str] = {}
    for map_key, map_value in value.items():
        if not isinstance(map_key, str) or not isinstance(map_value, str):
            raise ConfigError(f"Invalid '{key}' mapping; keys/values must be strings.")
        parsed[map_key] = map_value
    return parsed


def _parse_database(data: dict[str, Any]) -> DatabaseConfig:
    return DatabaseConfig(
        host=_require_str(data, "host"),
        port=_optional_int(data, "port", 5432),
        name=_require_str(data, "name"),
        user=_require_str(data, "user"),
        password=_require_str(data, "password"),
        sslmode=_optional_str(data, "sslmode"),
        connect_timeout=_optional_int(data, "connect_timeout", 10),
    )


def _parse_s3(data: dict[str, Any]) -> S3Config:
    return S3Config(
        bucket=_require_str(data, "bucket"),
        prefix=_optional_str(data, "prefix", DEFAULT_S3_PREFIX),
        region=_optional_str(data, "region") or None,
        access_key_id=_optional_str(data, "access_key_id") or None,
        secret_access_key=_optional_str(data, "secret_access_key") or None,
        session_token=_optional_str(data, "session_token") or None,
        endpoint_url=_optional_str(data, "endpoint_url") or None,
        use_ssl=_optional_bool(data, "use_ssl", True),
        verify_ssl=_optional_bool(data, "verify_ssl", True),
        addressing_style=_optional_str(data, "addressing_style"),
        sse=_optional_str(data, "sse"),
        kms_key_id=_optional_str(data, "kms_key_id"),
    )


def _parse_parquet(data: dict[str, Any]) -> ParquetConfig:
    encryption_data = data.get("encryption", {})
    if encryption_data is None:
        encryption_data = {}
    if not isinstance(encryption_data, dict):
        raise ConfigError("Invalid 'parquet.encryption' section; must be mapping.")

    encryption = ParquetEncryptionConfig(
        enabled=_optional_bool(encryption_data, "enabled", False),
        footer_key=_optional_str(encryption_data, "footer_key"),
        column_keys=_optional_str_map(encryption_data, "column_keys"),
        key_map=_optional_str_map(encryption_data, "key_map"),
    )

    return ParquetConfig(
        compression=_optional_str(data, "compression", DEFAULT_COMPRESSION),
        encryption=encryption,
    )


def _parse_archive(data: dict[str, Any]) -> ArchiveConfig:
    tables = data.get("tables")
    if not isinstance(tables, list) or not tables:
        raise ConfigError("'archive.tables' must be a non-empty list.")
    cleaned_tables = []
    for table in tables:
        if not isinstance(table, str) or not table.strip():
            raise ConfigError("'archive.tables' must contain only strings.")
        cleaned_tables.append(table.strip())

    cutoff = _optional_str(data, "cutoff_exclusive", DEFAULT_CUTOFF_EXCLUSIVE)
    base_dir = Path(_optional_str(data, "base_dir", str(REQUIRED_BASE_DIR))).resolve()
    ensure_under_base_dir(base_dir)

    return ArchiveConfig(
        tables=cleaned_tables,
        cutoff_exclusive=parse_utc_datetime(cutoff),
        base_dir=base_dir,
        fetch_size=_optional_int(data, "fetch_size", 10_000),
        overwrite=_optional_bool(data, "overwrite", False),
        dry_run=_optional_bool(data, "dry_run", False),
        perform_delete=_optional_bool(data, "perform_delete", False),
        delete_batch_size=_optional_int(data, "delete_batch_size", 10_000),
        order_by=_optional_str(data, "order_by", DEFAULT_ORDER_BY),
    )


def _parse_cron(data: Optional[dict[str, Any]]) -> CronConfig:
    if not data:
        return CronConfig()
    if not isinstance(data, dict):
        raise ConfigError("Invalid 'cron' section; must be mapping.")

    return CronConfig(
        enabled=_optional_bool(data, "enabled", False),
        schedule=_optional_str(data, "schedule"),
        command=_optional_str(data, "command"),
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Config file must contain a mapping: {path}")
    return raw


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    merged = dict(base)
    for key, value in override.items():
        if (
            key in merged
            and isinstance(merged[key], dict)
            and isinstance(value, dict)
        ):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _expand_env(val) for key, val in value.items()}
    if isinstance(value, list):
        return [_expand_env(item) for item in value]
    if isinstance(value, str):
        def _replace(match: re.Match[str]) -> str:
            raw_key = match.group(1)
            if ":" in raw_key:
                key, default = raw_key.split(":", 1)
            else:
                key, default = raw_key, ""
            return os.getenv(key, default)

        return _ENV_PATTERN.sub(_replace, value)
    return value


def load_config(path: Path) -> BackparqConfig:
    raw = _load_yaml(path)
    include = raw.pop("include", None)
    merged: dict[str, Any] = {}
    if include:
        include_paths = include if isinstance(include, list) else [include]
        for include_path in include_paths:
            if not isinstance(include_path, str):
                raise ConfigError("Config 'include' entries must be file paths.")
            merged = _merge_dicts(
                merged,
                _load_yaml((path.parent / include_path).resolve()),
            )
    merged = _merge_dicts(merged, raw)
    merged = _expand_env(merged)

    database = _parse_database(_require_section(merged, "database"))
    s3 = _parse_s3(_require_section(merged, "s3"))
    parquet = _parse_parquet(_require_section(merged, "parquet"))
    archive = _parse_archive(_require_section(merged, "archive"))
    cron = _parse_cron(merged.get("cron"))

    if not parquet.encryption.enabled:
        raise ConfigError("Parquet encryption must be enabled in the config.")

    if not parquet.encryption.footer_key:
        raise ConfigError("Parquet encryption enabled but 'footer_key' missing.")
    if not parquet.encryption.key_map:
        raise ConfigError("Parquet encryption enabled but 'key_map' missing.")
    if s3.addressing_style and s3.addressing_style not in {"path", "virtual", "auto"}:
        raise ConfigError(
            "Invalid s3.addressing_style; expected 'path', 'virtual', or 'auto'."
        )

    return BackparqConfig(
        database=database,
        s3=s3,
        parquet=parquet,
        archive=archive,
        cron=cron,
    )
