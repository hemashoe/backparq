"""
Unit tests for backparq.storage.parquet module.
"""

import tempfile
from pathlib import Path

from backparq.storage.parquet import (
    load_manifest,
    safe_mkdir,
    compute_sha256 as sha256_file,
    write_manifest,
)


class TestSha256File:
    """Tests for sha256_file function."""

    def test_sha256_known_content(self):
        """Test SHA256 of known content."""
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as f:
            f.write("Hello, World!")
            f.flush()

            # Known SHA256 for "Hello, World!"
            expected = "dffd6021bb2bd5b0af676290809ec3a53191dd81c7f70a4b28688a362182986f"
            result = sha256_file(Path(f.name))
            assert result == expected

    def test_sha256_empty_file(self):
        """Test SHA256 of empty file."""
        with tempfile.NamedTemporaryFile(delete=False) as f:
            # SHA256 of empty string
            expected = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
            result = sha256_file(Path(f.name))
            assert result == expected


class TestWriteText:
    """Tests for write_text via Path."""

    def test_write_and_read(self):
        """Test writing and reading text."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "test.txt"
            path.write_text("test content")
            assert path.read_text() == "test content"


class TestManifest:
    """Tests for manifest read/write functions."""

    def test_write_and_load_manifest(self):
        """Test writing and loading manifest."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            data = {"table": "public.events", "rows": 1000}

            write_manifest(path, data)
            loaded = load_manifest(path)

            assert loaded == data

    def test_load_nonexistent_manifest(self):
        """Test loading nonexistent manifest returns None."""
        result = load_manifest(Path("/nonexistent/manifest.json"))
        assert result is None

    def test_manifest_atomic_write(self):
        """Test manifest write is atomic (uses temp file)."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "manifest.json"
            data = {"key": "value"}

            write_manifest(path, data)

            # Should not have .tmp file
            assert not (path.with_suffix(".json.tmp")).exists()
            assert path.exists()


class TestSafeMkdir:
    """Tests for safe_mkdir function."""

    def test_create_nested_dirs(self):
        """Test creating nested directories."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "a" / "b" / "c"
            safe_mkdir(path)
            assert path.exists()
            assert path.is_dir()

    def test_existing_dir_ok(self):
        """Test that existing directory doesn't raise."""
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir)
            safe_mkdir(path)  # Should not raise
            assert path.exists()
