"""Tests for catalog adapter."""

import datetime as dt
import tempfile
from pathlib import Path

import pytest

from backparq.adapters.catalog import Catalog, ChunkState, RunStatus


class TestCatalog:
    """Tests for SQLite catalog."""

    @pytest.fixture
    def catalog(self):
        """Create a temporary catalog for testing."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_catalog.db"
            yield Catalog(db_path)

    def test_init_creates_schema(self, catalog):
        """Test that catalog initializes with correct schema."""
        stats = catalog.get_stats()
        assert stats["total_chunks"] == 0
        assert stats["total_runs"] == 0

    def test_transition_creates_chunk(self, catalog):
        """Test creating a new chunk via transition."""
        chunk_id = "public_events_2024-01"
        catalog.transition(
            chunk_id,
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
            sha256="abc123",
            row_count=1000,
        )

        state = catalog.get_state(chunk_id)
        assert state == ChunkState.EXPORTED

        chunk = catalog.get_chunk(chunk_id)
        assert chunk is not None
        assert chunk["table_name"] == "public.events"
        assert chunk["sha256"] == "abc123"
        assert chunk["row_count"] == 1000

    def test_transition_updates_existing(self, catalog):
        """Test updating an existing chunk."""
        chunk_id = "public_events_2024-01"

        # Create
        catalog.transition(
            chunk_id,
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
        )

        # Update
        catalog.transition(chunk_id, ChunkState.UPLOADED, s3_key="s3://bucket/key")

        state = catalog.get_state(chunk_id)
        assert state == ChunkState.UPLOADED

        chunk = catalog.get_chunk(chunk_id)
        assert chunk["s3_key"] == "s3://bucket/key"

    def test_list_chunks_filters(self, catalog):
        """Test listing chunks with filters."""
        # Create multiple chunks
        catalog.transition(
            "events_01",
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
        )
        catalog.transition(
            "events_02",
            ChunkState.UPLOADED,
            table_name="public.events",
            start_ts="2024-02-01T00:00:00Z",
            end_ts="2024-03-01T00:00:00Z",
        )
        catalog.transition(
            "users_01",
            ChunkState.EXPORTED,
            table_name="public.users",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
        )

        # Filter by table
        events = catalog.list_chunks(table_name="public.events")
        assert len(events) == 2

        # Filter by state
        exported = catalog.list_chunks(state=ChunkState.EXPORTED)
        assert len(exported) == 2

        # Filter by both
        events_exported = catalog.list_chunks(
            table_name="public.events", state=ChunkState.EXPORTED
        )
        assert len(events_exported) == 1

    def test_start_finish_run(self, catalog):
        """Test run tracking."""
        run_id = catalog.start_run("offload", config_hash="hash123")
        assert run_id is not None

        # Check run is in history
        history = catalog.get_history()
        assert len(history) == 1
        assert history[0]["id"] == run_id
        assert history[0]["status"] == RunStatus.RUNNING.value

        # Finish run
        catalog.finish_run(run_id, RunStatus.COMPLETED)

        history = catalog.get_history()
        assert history[0]["status"] == RunStatus.COMPLETED.value
        assert history[0]["finished_at"] is not None

    def test_get_stats(self, catalog):
        """Test statistics."""
        catalog.transition(
            "chunk1",
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
        )
        catalog.transition(
            "chunk2",
            ChunkState.UPLOADED,
            table_name="public.events",
            start_ts="2024-02-01T00:00:00Z",
            end_ts="2024-03-01T00:00:00Z",
        )

        catalog.start_run("backup")

        stats = catalog.get_stats()
        assert stats["total_chunks"] == 2
        assert stats["total_runs"] == 1
        assert stats["chunks_by_state"][ChunkState.EXPORTED.value] == 1
        assert stats["chunks_by_state"][ChunkState.UPLOADED.value] == 1

    def test_context_manager(self):
        """Test catalog works as context manager."""
        with tempfile.TemporaryDirectory() as tmpdir:
            db_path = Path(tmpdir) / "test_catalog.db"
            with Catalog(db_path) as catalog:
                catalog.transition(
                    "chunk1",
                    ChunkState.EXPORTED,
                    table_name="public.events",
                    start_ts="2024-01-01T00:00:00Z",
                    end_ts="2024-02-01T00:00:00Z",
                )
                assert catalog.get_state("chunk1") == ChunkState.EXPORTED
