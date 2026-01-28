"""
Unit tests for backparq.archive module.
"""

import datetime as dt
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from backparq.archive import (
    _s3_extra_args,
    chunk_paths,
    is_shutdown_requested,
    request_shutdown,
    s3_key_for_chunk,
)
from backparq.db import ChunkSpec


class TestChunkPaths:
    """Tests for chunk_paths function."""

    def test_chunk_paths_structure(self):
        """Test that chunk_paths returns correct structure."""
        base_dir = Path("/tmp/backparq")
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        with patch("backparq.archive.safe_mkdir"):
            final, inprogress, sha, manifest = chunk_paths(base_dir, chunk)

        assert "public_events" in str(final)
        assert "year=2024" in str(final)
        assert "month=03" in str(final)
        assert final.suffix == ".parquet"
        assert ".inprogress" in str(inprogress)
        assert sha.suffix == ".sha256"
        assert "manifest.json" in str(manifest)


class TestS3KeyForChunk:
    """Tests for s3_key_for_chunk function."""

    def test_offload_mode_key(self):
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

    def test_backup_mode_key(self):
        """Test S3 key generation for backup mode."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        key = s3_key_for_chunk("db-archive", chunk, "backup", run_id="2024-03-01_120000")

        assert key.startswith("db-archive/backups/2024-03-01_120000/")
        assert "public_events" in key

    def test_backup_mode_requires_run_id(self):
        """Test that backup mode without run_id raises ValueError."""
        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 3, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 4, 1, tzinfo=dt.timezone.utc),
        )

        with pytest.raises(ValueError, match="run_id"):
            s3_key_for_chunk("db-archive", chunk, "backup")


class TestShutdownHandling:
    """Tests for graceful shutdown handling."""

    def test_shutdown_flag_default_false(self):
        """Test shutdown flag is False by default."""
        # Reset the flag
        from backparq.archive import _shutdown_requested

        _shutdown_requested.clear()

        assert not is_shutdown_requested()

    def test_request_shutdown_sets_flag(self):
        """Test request_shutdown sets the flag."""
        from backparq.archive import _shutdown_requested

        _shutdown_requested.clear()

        request_shutdown()
        assert is_shutdown_requested()

        # Clean up
        _shutdown_requested.clear()


class TestS3ExtraArgs:
    """Tests for _s3_extra_args function."""

    def test_no_sse(self):
        """Test no extra args when SSE not configured."""
        config = MagicMock()
        config.sse = None
        config.kms_key_id = None

        result = _s3_extra_args(config)
        assert result == {}

    def test_sse_s3(self):
        """Test SSE-S3 encryption."""
        config = MagicMock()
        config.sse = "AES256"
        config.kms_key_id = None

        result = _s3_extra_args(config)
        assert result == {"ServerSideEncryption": "AES256"}

    def test_sse_kms(self):
        """Test SSE-KMS encryption."""
        config = MagicMock()
        config.sse = "aws:kms"
        config.kms_key_id = "alias/my-key"

        result = _s3_extra_args(config)
        assert result == {
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "alias/my-key",
        }
