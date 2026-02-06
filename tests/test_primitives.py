"""Tests for primitives."""

import datetime as dt
import hashlib
import tempfile
from pathlib import Path

import pyarrow as pa
import pytest

from backparq.db.operations import ChunkSpec
from backparq.primitives import (
    add_months,
    apply_masking,
    chunk_paths,
    compute_sha256,
    get_chunk_filename,
    month_floor,
    normalize_dt,
    s3_key_for_chunk,
)


class TestChecksum:
    """Tests for checksum computation."""

    def test_compute_sha256(self):
        """Test SHA256 computation."""
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as f:
            f.write("test content")
            f.flush()
            path = Path(f.name)

        try:
            sha = compute_sha256(path)
            expected = hashlib.sha256(b"test content").hexdigest()
            assert sha == expected
        finally:
            path.unlink()


class TestChunking:
    """Tests for time/path functions."""

    def test_normalize_dt(self):
        """Test datetime normalization to UTC."""
        # Naive datetime
        naive = dt.datetime(2024, 1, 1, 12, 0, 0)
        normalized = normalize_dt(naive)
        assert normalized.tzinfo == dt.timezone.utc

        # Already UTC
        utc = dt.datetime(2024, 1, 1, 12, 0, 0, tzinfo=dt.timezone.utc)
        normalized = normalize_dt(utc)
        assert normalized == utc

    def test_month_floor(self):
        """Test month floor."""
        value = dt.datetime(2024, 3, 15, 10, 30, 0, tzinfo=dt.timezone.utc)
        floored = month_floor(value)
        assert floored == dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc)

    def test_add_months(self):
        """Test adding months."""
        start = dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc)

        # Add 1 month
        result = add_months(start, 1)
        assert result == dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc)

        # Add 12 months (year boundary)
        result = add_months(start, 12)
        assert result == dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)

        # Add negative months
        result = add_months(start, -1)
        assert result == dt.datetime(2023, 12, 1, tzinfo=dt.timezone.utc)

    def test_get_chunk_filename(self):
        """Test chunk filename generation."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        filename = get_chunk_filename(chunk)
        assert "public_events" in filename
        assert "2024-03" in filename
        assert filename.endswith(".parquet")

    def test_chunk_paths(self):
        """Test chunk path generation."""
        base_dir = Path("/tmp/backparq")
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        final, inprogress, sha, manifest = chunk_paths(base_dir, chunk)

        assert "public_events" in str(final)
        assert "year=2024" in str(final)
        assert "month=03" in str(final)
        assert final.suffix == ".parquet"
        assert ".inprogress" in str(inprogress)
        assert sha.suffix == ".sha256"
        assert "manifest.json" in str(manifest)

    def test_s3_key_for_chunk_offload(self):
        """Test S3 key generation for offload mode."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        key = s3_key_for_chunk("db-archive", chunk, "offload")

        assert key.startswith("db-archive/archive/")
        assert "public_events" in key
        assert "year=2024" in key
        assert "month=03" in key
        assert key.endswith(".parquet")

    def test_s3_key_for_chunk_backup(self):
        """Test S3 key generation for backup mode."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        key = s3_key_for_chunk("db-archive", chunk, "backup", run_id="2024-03-01_120000")

        assert key.startswith("db-archive/backups/2024-03-01_120000/")
        assert "public_events" in key

    def test_s3_key_backup_requires_run_id(self):
        """Test that backup mode without run_id raises ValueError."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        with pytest.raises(ValueError, match="run_id"):
            s3_key_for_chunk("db-archive", chunk, "backup")


class TestMasking:
    """Tests for data masking."""

    def test_apply_masking_hash(self):
        """Test hash masking."""
        table = pa.table({"email": ["user@example.com", "admin@example.com"]})

        masked = apply_masking(table, {"email": "hash"})

        # Check that values are hashed
        emails = masked["email"].to_pylist()
        assert emails[0] != "user@example.com"
        assert emails[1] != "admin@example.com"
        assert len(emails[0]) == 64  # SHA256 hex length

    def test_apply_masking_redact(self):
        """Test redact masking."""
        table = pa.table({"ssn": ["123-45-6789", "987-65-4321"]})

        masked = apply_masking(table, {"ssn": "redact"})

        ssns = masked["ssn"].to_pylist()
        assert ssns[0] == "***REDACTED***"
        assert ssns[1] == "***REDACTED***"

    def test_apply_masking_partial(self):
        """Test partial masking."""
        table = pa.table({"phone": ["555-1234", "555-5678"]})

        masked = apply_masking(table, {"phone": "partial"})

        phones = masked["phone"].to_pylist()
        assert phones[0] == "****1234"
        assert phones[1] == "****5678"

    def test_apply_masking_no_rules(self):
        """Test that no masking returns original table."""
        table = pa.table({"col1": [1, 2, 3]})

        masked = apply_masking(table, {})

        assert masked == table

    def test_apply_masking_null_values(self):
        """Test masking with null values."""
        table = pa.table({"email": ["user@example.com", None]})

        masked = apply_masking(table, {"email": "hash"})

        emails = masked["email"].to_pylist()
        assert emails[0] is not None  # Hashed
        assert emails[1] is None  # Null preserved
