"""
Unit tests for backparq.config module.
"""

import tempfile
from pathlib import Path

import pytest

from backparq.config import (
    ConfigError,
    TableConfig,
    _parse_tables,
    load_config,
    parse_cutoff,
    parse_utc_datetime,
)


class TestParseUtcDatetime:
    """Tests for parse_utc_datetime function."""

    def test_parse_iso_format(self):
        """Test parsing ISO8601 datetime string."""
        # Use +00:00 instead of Z for Python 3.9 compatibility
        dt = parse_utc_datetime("2024-01-15T10:30:00+00:00")
        assert dt.year == 2024
        assert dt.month == 1
        assert dt.day == 15

    def test_parse_date_only(self):
        """Test parsing date-only string."""
        dt = parse_utc_datetime("2024-06-01")
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 1

    def test_parse_invalid_raises(self):
        """Test that invalid date raises an exception."""
        with pytest.raises((ConfigError, ValueError)):
            parse_utc_datetime("not-a-date")


class TestParseCutoff:
    """Tests for parse_cutoff function (relative and ISO8601)."""

    def test_parse_relative_days(self):
        """Test parsing relative days format."""
        result = parse_cutoff("-30d")
        assert result is not None
        # Should be approximately 30 days ago (allow some tolerance)
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        delta = (now - result).days
        assert 29 <= delta <= 31

    def test_parse_relative_months(self):
        """Test parsing relative months format."""
        result = parse_cutoff("-6m")
        assert result is not None

    def test_parse_relative_years(self):
        """Test parsing relative years format."""
        result = parse_cutoff("-1y")
        assert result is not None

    def test_parse_iso8601_fallback(self):
        """Test that ISO8601 dates still work."""
        result = parse_cutoff("2024-06-01")
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 1


class TestParseTables:
    """Tests for _parse_tables function."""

    def test_simple_string_tables(self):
        """Test parsing simple string table names."""
        tables = ["public.events", "public.users"]
        result = _parse_tables(tables)

        assert len(result) == 2
        assert result[0].name == "public.events"
        assert result[0].primary_key == "id"
        assert result[1].name == "public.users"

    def test_dict_tables_with_primary_key(self):
        """Test parsing dict format with custom primary key."""
        tables = [
            {"table": "public.orders", "primary_key": "order_id"},
            {"table": "public.logs", "primary_key": "log_id"},
        ]
        result = _parse_tables(tables)

        assert len(result) == 2
        assert result[0].name == "public.orders"
        assert result[0].primary_key == "order_id"
        assert result[1].name == "public.logs"
        assert result[1].primary_key == "log_id"

    def test_mixed_format_tables(self):
        """Test parsing mixed string and dict tables."""
        tables = [
            "public.events",
            {"table": "public.orders", "primary_key": "order_id"},
        ]
        result = _parse_tables(tables)

        assert len(result) == 2
        assert result[0].name == "public.events"
        assert result[0].primary_key == "id"
        assert result[1].name == "public.orders"
        assert result[1].primary_key == "order_id"

    def test_empty_tables_raises(self):
        """Test that empty tables list raises ConfigError."""
        with pytest.raises(ConfigError, match="tables"):
            _parse_tables([])

    def test_none_tables_raises(self):
        """Test that None tables raises ConfigError."""
        with pytest.raises(ConfigError, match="tables"):
            _parse_tables(None)


class TestTableConfig:
    """Tests for TableConfig dataclass."""

    def test_default_primary_key(self):
        """Test default primary key is 'id'."""
        tc = TableConfig(name="public.events")
        assert tc.primary_key == "id"

    def test_custom_primary_key(self):
        """Test custom primary key."""
        tc = TableConfig(name="public.orders", primary_key="order_id")
        assert tc.primary_key == "order_id"


class TestLoadConfig:
    """Tests for load_config function."""

    def test_load_minimal_config(self):
        """Test loading a minimal valid config."""
        config_yaml = """
database:
  host: localhost
  port: 5432
  name: testdb
  user: postgres
  password: secret

s3:
  bucket: my-bucket
  prefix: archives

archive:
  tables:
    - public.events
  mode: offload
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(config_yaml)
            f.flush()

            config = load_config(Path(f.name))

            assert config.database.host == "localhost"
            assert config.database.port == 5432
            assert config.s3.bucket == "my-bucket"
            assert len(config.archive.tables) == 1
            assert config.archive.mode == "offload"

    def test_load_config_missing_file(self):
        """Test loading nonexistent file raises ConfigError."""
        with pytest.raises(ConfigError, match="not found"):
            load_config(Path("/nonexistent/config.yaml"))

    def test_load_config_with_env_expansion(self):
        """Test environment variable expansion."""
        import os

        os.environ["TEST_DB_PASSWORD"] = "supersecret"

        config_yaml = """
database:
  host: localhost
  port: 5432
  name: testdb
  user: postgres
  password: "${TEST_DB_PASSWORD}"

s3:
  bucket: my-bucket
  prefix: archives

archive:
  tables:
    - public.events
  mode: backup
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(config_yaml)
            f.flush()

            config = load_config(Path(f.name))
            assert config.database.password == "supersecret"

        del os.environ["TEST_DB_PASSWORD"]

    def test_load_config_per_table_primary_key(self):
        """Test loading config with per-table primary keys."""
        config_yaml = """
database:
  host: localhost
  port: 5432
  name: testdb
  user: postgres
  password: secret

s3:
  bucket: my-bucket
  prefix: archives

archive:
  tables:
    - public.events
    - table: public.orders
      primary_key: order_id
  mode: offload
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(config_yaml)
            f.flush()

            config = load_config(Path(f.name))

            assert len(config.archive.tables) == 2
            assert config.archive.tables[0].primary_key == "id"
            assert config.archive.tables[1].primary_key == "order_id"

            # Test get_table_config method
            orders_config = config.archive.get_table_config("public.orders")
            assert orders_config is not None
            assert orders_config.primary_key == "order_id"

    def test_load_config_invalid_compression(self):
        """Test loading config with invalid compression raises ConfigError."""
        config_yaml = """
database:
  host: localhost
  port: 5432
  name: testdb
  user: postgres
  password: secret
s3:
  bucket: my-bucket
archive:
  tables: [public.events]
  mode: offload
parquet:
  compression: invalid_codec
"""
        with tempfile.NamedTemporaryFile(suffix=".yaml", delete=False, mode="w") as f:
            f.write(config_yaml)
            f.flush()

            with pytest.raises(ConfigError, match="Invalid compression"):
                load_config(Path(f.name))
