"""
Unit tests for backparq.db module.
"""

import datetime as dt

from backparq.db import (
    ChunkSpec,
    _parse_table_name,
    add_months,
    month_floor,
)


class TestMonthFloor:
    """Tests for month_floor function."""

    def test_floor_middle_of_month(self):
        """Test flooring date in middle of month."""
        d = dt.datetime(2024, 3, 15, 10, 30, 0, tzinfo=dt.timezone.utc)
        result = month_floor(d)

        assert result.year == 2024
        assert result.month == 3
        assert result.day == 1
        assert result.hour == 0
        assert result.minute == 0

    def test_floor_first_of_month(self):
        """Test flooring date on first of month."""
        d = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        result = month_floor(d)

        assert result.year == 2024
        assert result.month == 1
        assert result.day == 1


class TestAddMonths:
    """Tests for add_months function."""

    def test_add_one_month(self):
        """Test adding one month."""
        d = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        result = add_months(d, 1)

        assert result.year == 2024
        assert result.month == 2
        assert result.day == 1

    def test_add_months_year_wrap(self):
        """Test adding months that wraps to next year."""
        d = dt.datetime(2024, 11, 1, tzinfo=dt.timezone.utc)
        result = add_months(d, 3)

        assert result.year == 2025
        assert result.month == 2
        assert result.day == 1

    def test_add_twelve_months(self):
        """Test adding 12 months."""
        d = dt.datetime(2024, 6, 1, tzinfo=dt.timezone.utc)
        result = add_months(d, 12)

        assert result.year == 2025
        assert result.month == 6


class TestParseTableName:
    """Tests for _parse_table_name function."""

    def test_parse_qualified_name(self):
        """Test parsing schema.table format."""
        schema, table = _parse_table_name("public.events")
        assert schema == "public"
        assert table == "events"

    def test_parse_unqualified_name(self):
        """Test parsing unqualified table name."""
        schema, table = _parse_table_name("events")
        assert schema is None
        assert table == "events"

    def test_parse_multiple_dots(self):
        """Test parsing name with multiple dots uses first as schema."""
        schema, table = _parse_table_name("myschema.my_table.extra")
        # Should split on first dot only
        assert schema == "myschema"
        assert table == "my_table.extra"


class TestChunkSpec:
    """Tests for ChunkSpec dataclass."""

    def test_chunk_spec_creation(self):
        """Test creating a ChunkSpec."""
        start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc)

        chunk = ChunkSpec(table="public.events", start=start, end=end)

        assert chunk.table == "public.events"
        assert chunk.start == start
        assert chunk.end == end

    def test_chunk_spec_str(self):
        """Test ChunkSpec string representation."""
        start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)
        end = dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc)

        chunk = ChunkSpec(table="public.events", start=start, end=end)
        s = str(chunk)

        assert "public.events" in s
        assert "2024-01" in s
