from __future__ import annotations

import datetime as dt
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

import yaml

logger = logging.getLogger(__name__)

# Defaults - no longer hardcoded restrictions
DEFAULT_BASE_DIR = Path.cwd() / "backparq-data"
DEFAULT_CUTOFF_EXCLUSIVE = ""  # Empty means no cutoff (full table)
DEFAULT_S3_PREFIX = "db-archive"
DEFAULT_ORDER_BY = "created_at"
DEFAULT_COMPRESSION = "zstd"
DEFAULT_PRIMARY_KEY = "id"


from backparq.exceptions import ConfigError


@dataclass(frozen=True)
class DatabaseConfig:
    """PostgreSQL connection configuration."""

    host: str
    port: int
    name: str
    user: str
    password: str
    sslmode: str = ""
    connect_timeout: int = 10

    @staticmethod
    def _quote_libpq(value: str) -> str:
        """Quote a libpq connection string value if it contains special chars."""
        if not value or any(c in value for c in (" ", "'", "\\", "=")):
            escaped = value.replace("\\", "\\\\").replace("'", "\\'")
            return f"'{escaped}'"
        return value

    def dsn(self) -> str:
        parts = [
            f"host={self._quote_libpq(self.host)}",
            f"port={self.port}",
            f"dbname={self._quote_libpq(self.name)}",
            f"user={self._quote_libpq(self.user)}",
            f"password={self._quote_libpq(self.password)}",
            f"connect_timeout={self.connect_timeout}",
        ]
        if self.sslmode:
            parts.append(f"sslmode={self._quote_libpq(self.sslmode)}")
        return " ".join(parts)

    def duckdb_dsn(self) -> str:
        """Build a DSN string safe for DuckDB postgres_scanner.

        DuckDB's postgres extension uses libpq under the hood, so we build
        a clean libpq connection string from individual components, avoiding
        any fragile string manipulation of existing DSN strings.
        """
        parts = [
            f"host={self._quote_libpq(self.host)}",
            f"port={self.port}",
            f"dbname={self._quote_libpq(self.name)}",
            f"user={self._quote_libpq(self.user)}",
            f"password={self._quote_libpq(self.password)}",
        ]
        if self.sslmode:
            parts.append(f"sslmode={self._quote_libpq(self.sslmode)}")
        return " ".join(parts)


@dataclass(frozen=True)
class S3Config:
    """S3 storage configuration."""

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
    """Parquet encryption configuration (optional)."""

    enabled: bool = False
    footer_key: str = ""
    column_keys: dict[str, str] = field(default_factory=dict)
    key_map: dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class ParquetConfig:
    """Parquet file configuration."""

    compression: str = DEFAULT_COMPRESSION
    row_group_size: int = 100_000
    encryption: ParquetEncryptionConfig = field(default_factory=ParquetEncryptionConfig)


@dataclass(frozen=True)
class RetentionConfig:
    """Retention policy for pruning old backups."""

    enabled: bool = False
    days: int = 0
    months: int = 0

    @property
    def total_days(self) -> int:
        """Total retention in days (months converted to 30 days each)."""
        return self.days + (self.months * 30)


@dataclass(frozen=True)
class TableConfig:
    """Per-table configuration including primary key."""

    name: str
    primary_key: str = DEFAULT_PRIMARY_KEY
    order_by: str = DEFAULT_ORDER_BY
    masking: dict[str, str] = field(default_factory=dict)

    def __str__(self) -> str:
        return self.name


@dataclass(frozen=True)
class ArchiveConfig:
    """Archive job configuration."""

    tables: list[TableConfig]
    mode: str  # "backup" or "offload"
    cutoff_exclusive: Optional[dt.datetime]
    base_dir: Path
    fetch_size: int = 10_000
    overwrite: bool = False
    dry_run: bool = False
    perform_delete: bool = False
    offload_strategy: str = "delete"
    delete_batch_size: int = 10_000
    order_by: str = DEFAULT_ORDER_BY
    concurrency: int = 1
    chunk_concurrency: int = 1
    chunk_rows: int = 500_000
    vacuum: bool = False
    retention: RetentionConfig = field(default_factory=RetentionConfig)

    def __post_init__(self) -> None:
        if self.mode not in ("backup", "offload"):
            raise ConfigError(f"Invalid mode '{self.mode}'. Must be 'backup' or 'offload'.")
        if self.mode == "backup" and self.perform_delete:
            raise ConfigError(
                "Mode 'backup' cannot be combined with 'perform_delete'. "
                "Backup mode creates full snapshots without deletion."
            )

    def get_table_config(self, table_name: str) -> TableConfig:
        """Get config for a specific table by name."""
        for tc in self.tables:
            if tc.name == table_name:
                return tc
        raise ConfigError(f"Table '{table_name}' not found in config.")

    @property
    def table_names(self) -> list[str]:
        """Return list of table names for iteration."""
        return [t.name for t in self.tables]


@dataclass(frozen=True)
class CronConfig:
    """Cron scheduling configuration."""

    enabled: bool = False
    schedule: str = ""
    command: str = ""


@dataclass(frozen=True)
class NotificationConfig:
    """Notification configuration."""

    enabled: bool = False
    urls: list[str] = field(default_factory=list)
    on_success: bool = False
    on_failure: bool = True


@dataclass(frozen=True)
class BackparqConfig:
    """Root configuration object."""

    database: DatabaseConfig
    s3: S3Config
    parquet: ParquetConfig
    archive: ArchiveConfig
    cron: CronConfig = field(default_factory=CronConfig)
    notifications: Optional[NotificationConfig] = None


# =============================================================================
# Parsing Utilities
# =============================================================================


_RELATIVE_CUTOFF = re.compile(r"^(-?\d+)([dDwWmMyY])$")


def parse_utc_datetime(value: str) -> dt.datetime:
    """Parse a datetime string to UTC timezone-aware datetime."""
    if not value:
        raise ConfigError("Empty datetime string")
    if "T" in value:
        parsed = dt.datetime.fromisoformat(value)
    else:
        parsed = dt.datetime.fromisoformat(value + "T00:00:00")
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc)


def parse_cutoff(value: str) -> dt.datetime:
    """
    Parse cutoff as ISO8601 date or relative format.

    Relative formats: -30d (30 days ago), -90d, -6m (6 months), -1y (1 year).
    Supports: d/days, w/weeks, m/months, y/years. Negative = past.
    """
    if not value:
        raise ConfigError("Empty cutoff string")
    value = value.strip()
    match = _RELATIVE_CUTOFF.match(value)
    if match:
        num = int(match.group(1))
        unit = match.group(2).lower()
        now = dt.datetime.now(dt.timezone.utc)
        if unit in ("d",):
            return now - dt.timedelta(days=abs(num))
        if unit in ("w",):
            return now - dt.timedelta(weeks=abs(num))
        if unit in ("m",):
            # Approximate: 30 days per month
            return now - dt.timedelta(days=abs(num) * 30)
        if unit in ("y",):
            return now - dt.timedelta(days=abs(num) * 365)
    return parse_utc_datetime(value)


def _require_section(data: dict[str, Any], key: str) -> dict[str, Any]:
    """Require a config section to exist and be a dict."""
    value = data.get(key)
    if isinstance(value, dict):
        return value
    raise ConfigError(f"Missing or invalid '{key}' section in config.")


def _require_str(data: dict[str, Any], key: str) -> str:
    """Require a non-empty string value."""
    value = data.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"Missing or invalid '{key}' value in config.")
    return value.strip()


def _optional_str(data: dict[str, Any], key: str, default: str = "") -> str:
    """Get optional string value with default."""
    value = data.get(key, default)
    if value is None:
        return default
    if not isinstance(value, str):
        raise ConfigError(f"Invalid '{key}' value in config; must be string.")
    return value.strip()


def _optional_int(data: dict[str, Any], key: str, default: int) -> int:
    """Get optional integer value with default."""
    value = data.get(key, default)
    if isinstance(value, bool):
        raise ConfigError(f"Invalid '{key}' value in config (bool not allowed).")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"Invalid '{key}' value in config.") from exc


def _optional_bool(data: dict[str, Any], key: str, default: bool = False) -> bool:
    """Get optional boolean value with default."""
    value = data.get(key, default)
    if isinstance(value, bool):
        return value
    raise ConfigError(f"Invalid '{key}' value in config; must be boolean.")


def _optional_str_map(data: dict[str, Any], key: str) -> dict[str, str]:
    """Get optional string->string mapping."""
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


# =============================================================================
# Section Parsers
# =============================================================================


def _parse_database(data: dict[str, Any]) -> DatabaseConfig:
    """Parse database configuration section."""
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
    """Parse S3 configuration section."""
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
    """Parse Parquet configuration section."""
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

    compression = _optional_str(data, "compression", DEFAULT_COMPRESSION).lower()
    allowed_compressions = {"snappy", "gzip", "brotli", "lz4", "zstd", "none"}
    if compression not in allowed_compressions:
        raise ConfigError(
            f"Invalid compression '{compression}'. Allowed: {', '.join(sorted(allowed_compressions))}"
        )

    return ParquetConfig(
        compression=compression,
        row_group_size=_optional_int(data, "row_group_size", 100_000),
        encryption=encryption,
    )


def _parse_retention(data: dict[str, Any]) -> RetentionConfig:
    """Parse retention configuration section."""
    if not data:
        return RetentionConfig()

    return RetentionConfig(
        enabled=_optional_bool(data, "enabled", False),
        days=_optional_int(data, "days", 0),
        months=_optional_int(data, "months", 0),
    )


def _parse_tables(tables_data: list) -> list[TableConfig]:
    """
    Parse tables configuration.

    Supports both simple strings and dicts with per-table config:
    tables:
      - public.events                    # Simple string
      - table: public.orders             # Dict with options
        primary_key: order_id
    """
    if not isinstance(tables_data, list) or not tables_data:
        raise ConfigError("'archive.tables' must be a non-empty list.")

    parsed_tables: list[TableConfig] = []
    for item in tables_data:
        if isinstance(item, str):
            if not item.strip():
                raise ConfigError("'archive.tables' must not contain empty strings.")
            parsed_tables.append(TableConfig(name=item.strip()))
        elif isinstance(item, dict):
            table_name = item.get("table") or item.get("name")
            if not isinstance(table_name, str) or not table_name.strip():
                raise ConfigError("Table config must have 'table' or 'name' key with string value.")
            pk = _optional_str(item, "primary_key", DEFAULT_PRIMARY_KEY)
            order_by = _optional_str(item, "order_by", DEFAULT_ORDER_BY)
            masking = _optional_str_map(item, "masking")
            parsed_tables.append(
                TableConfig(
                    name=table_name.strip(),
                    primary_key=pk,
                    order_by=order_by,
                    masking=masking,
                )
            )
        else:
            raise ConfigError(f"Invalid table entry: {item}. Must be string or dict.")

    return parsed_tables


def _parse_archive(data: dict[str, Any]) -> ArchiveConfig:
    """Parse archive configuration section."""
    tables = _parse_tables(data.get("tables", []))
    mode = _optional_str(data, "mode", "offload").lower()

    # Cutoff is optional - supports cutoff or cutoff_exclusive (alias)
    # Formats: ISO8601 (2024-01-15) or relative (-30d, -90d, -6m, -1y)
    cutoff_raw = _optional_str(data, "cutoff_exclusive", "") or _optional_str(data, "cutoff", "")
    cutoff = parse_cutoff(cutoff_raw) if cutoff_raw else None

    # Base directory - configurable, defaults to ./backparq-data
    base_dir_str = _optional_str(data, "base_dir", str(DEFAULT_BASE_DIR))
    base_dir = Path(base_dir_str).resolve()

    # Ensure directory exists or can be created
    if not base_dir.exists():
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created base directory: {base_dir}")
        except OSError as e:
            raise ConfigError(f"Cannot create base directory {base_dir}: {e}") from e

    return ArchiveConfig(
        tables=tables,
        mode=mode,
        cutoff_exclusive=cutoff,
        base_dir=base_dir,
        fetch_size=_optional_int(data, "fetch_size", 10_000),
        overwrite=_optional_bool(data, "overwrite", False),
        dry_run=_optional_bool(data, "dry_run", False),
        perform_delete=_optional_bool(data, "perform_delete", False),
        offload_strategy=_optional_str(data, "offload_strategy", "delete").lower(),
        delete_batch_size=_optional_int(data, "delete_batch_size", 10_000),
        order_by=_optional_str(data, "order_by", DEFAULT_ORDER_BY),
        concurrency=_optional_int(data, "concurrency", 1),
        chunk_concurrency=_optional_int(data, "chunk_concurrency", 1),
        chunk_rows=_optional_int(data, "chunk_rows", 500_000),
        vacuum=_optional_bool(data, "vacuum", False),
        retention=_parse_retention(data.get("retention", {})),
    )


def _parse_cron(data: Optional[dict[str, Any]]) -> CronConfig:
    """Parse cron configuration section."""
    if not data:
        return CronConfig()
    if not isinstance(data, dict):
        raise ConfigError("Invalid 'cron' section; must be mapping.")

    return CronConfig(
        enabled=_optional_bool(data, "enabled", False),
        schedule=_optional_str(data, "schedule"),
        command=_optional_str(data, "command"),
    )


def _parse_notifications(data: Optional[dict[str, Any]]) -> Optional[NotificationConfig]:
    """Parse notification configuration section."""
    if not data:
        return None

    urls = data.get("urls", [])
    if isinstance(urls, str):
        urls = [urls]

    return NotificationConfig(
        enabled=True,
        urls=[u for u in urls if isinstance(u, str) and u],
        on_success=_optional_bool(data, "on_success", False),
        on_failure=_optional_bool(data, "on_failure", True),
    )


# =============================================================================
# YAML Loading and Environment Expansion
# =============================================================================


def _load_yaml(path: Path) -> dict[str, Any]:
    """Load and parse a YAML configuration file."""
    if not path.exists():
        raise ConfigError(f"Config file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8"))
    if raw is None:
        return {}
    if not isinstance(raw, dict):
        raise ConfigError(f"Config file must contain a mapping: {path}")
    return raw


def _merge_dicts(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    """Deep merge two dictionaries, with override taking precedence."""
    merged = dict(base)
    for key, value in override.items():
        if key in merged and isinstance(merged[key], dict) and isinstance(value, dict):
            merged[key] = _merge_dicts(merged[key], value)
        else:
            merged[key] = value
    return merged


_ENV_PATTERN = re.compile(r"\$\{([^}]+)\}")


def _expand_env(value: Any) -> Any:
    """Recursively expand ${VAR} and ${VAR:default} patterns."""
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


# =============================================================================
# Main Configuration Loader
# =============================================================================


def load_config(path: Path) -> BackparqConfig:
    """
    Load and validate configuration from a YAML file.

    Supports:
    - Environment variable expansion: ${VAR} or ${VAR:default}
    - Config file includes via 'include' key
    - Per-table primary key configuration
    - Optional encryption
    """
    logger.info(f"Loading configuration from: {path}")
    raw = _load_yaml(path)

    # Handle includes
    include = raw.pop("include", None)
    merged: dict[str, Any] = {}
    if include:
        include_paths = include if isinstance(include, list) else [include]
        for include_path in include_paths:
            if not isinstance(include_path, str):
                raise ConfigError("Config 'include' entries must be file paths.")
            include_file = (path.parent / include_path).resolve()
            logger.debug(f"Including config: {include_file}")
            merged = _merge_dicts(merged, _load_yaml(include_file))

    merged = _merge_dicts(merged, raw)
    merged = _expand_env(merged)

    # Parse sections
    database = _parse_database(_require_section(merged, "database"))
    s3 = _parse_s3(_require_section(merged, "s3"))
    parquet = _parse_parquet(merged.get("parquet", {}))
    archive = _parse_archive(_require_section(merged, "archive"))
    cron = _parse_cron(merged.get("cron"))
    notifications = _parse_notifications(merged.get("notifications"))

    # Validate encryption config (only if enabled)
    if parquet.encryption.enabled:
        if not parquet.encryption.footer_key:
            raise ConfigError("Parquet encryption enabled but 'footer_key' missing.")
        if not parquet.encryption.key_map:
            raise ConfigError("Parquet encryption enabled but 'key_map' missing.")
        logger.info("Parquet encryption is enabled")
    else:
        logger.debug("Parquet encryption is disabled")

    # Validate S3 addressing style
    if s3.addressing_style and s3.addressing_style not in {"path", "virtual", "auto"}:
        raise ConfigError("Invalid s3.addressing_style; expected 'path', 'virtual', or 'auto'.")

    logger.info(
        f"Configuration loaded: {len(archive.tables)} tables, "
        f"mode={archive.mode}, concurrency={archive.concurrency}"
    )

    return BackparqConfig(
        database=database,
        s3=s3,
        parquet=parquet,
        archive=archive,
        cron=cron,
        notifications=notifications,
    )
