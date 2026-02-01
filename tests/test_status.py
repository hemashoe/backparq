"""Tests for the status module."""



from backparq.utils.console import format_count, format_size
from backparq.status import _get_local_stats


class TestFormatSize:
    def test_bytes(self):
        assert format_size(500) == "500.0 B"

    def test_kilobytes(self):
        assert format_size(2048) == "2.0 KB"

    def test_megabytes(self):
        assert format_size(5 * 1024 * 1024) == "5.0 MB"

    def test_gigabytes(self):
        assert format_size(3 * 1024 * 1024 * 1024) == "3.0 GB"


class TestFormatCount:
    def test_small_number(self):
        assert format_count(123) == "123"

    def test_thousands(self):
        assert format_count(1234567) == "1,234,567"

    def test_zero(self):
        assert format_count(0) == "0"


class TestGetLocalStats:
    def test_nonexistent_directory(self, tmp_path):
        stats = _get_local_stats(tmp_path, "nonexistent_table")
        assert stats["chunk_count"] == 0
        assert stats["total_size"] == 0
        assert stats["total_rows"] == 0

    def test_empty_directory(self, tmp_path):
        table_dir = tmp_path / "parquet" / "public_events"
        table_dir.mkdir(parents=True)
        stats = _get_local_stats(tmp_path, "public.events")
        assert stats["chunk_count"] == 0

    def test_with_parquet_files(self, tmp_path):
        table_dir = tmp_path / "parquet" / "public_events" / "year=2024" / "month=01"
        table_dir.mkdir(parents=True)
        parquet_file = table_dir / "public_events_2024-01.parquet"
        parquet_file.write_bytes(b"fake parquet content")
        stats = _get_local_stats(tmp_path, "public.events")
        assert stats["chunk_count"] == 1
        assert stats["total_size"] > 0
        assert stats["min_date"] == "2024-01"
        assert stats["max_date"] == "2024-01"

    def test_ignores_inprogress_files(self, tmp_path):
        table_dir = tmp_path / "parquet" / "public_events" / "year=2024" / "month=01"
        table_dir.mkdir(parents=True)
        inprogress = table_dir / "public_events_2024-01.parquet.inprogress"
        inprogress.write_bytes(b"incomplete")
        stats = _get_local_stats(tmp_path, "public.events")
        assert stats["chunk_count"] == 0
