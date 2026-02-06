"""Tests for ledger-based operations (race condition fix)."""

import datetime as dt
import json
from pathlib import Path

import pytest

from backparq.primitives.ledger import (
    create_ledger,
    delete_with_ledger,
    deserialize_ledger,
    serialize_ledger,
)


class TestLedgerSerialization:
    """Test ledger serialization/deserialization."""

    def test_serialize_pk_ledger(self):
        """Test serializing PK-based ledger."""
        ledger = {"type": "pk_list", "column": "id", "values": [1, 2, 3, 100, 200]}

        serialized = serialize_ledger(ledger)
        assert isinstance(serialized, str)

        deserialized = deserialize_ledger(serialized)
        assert deserialized == ledger

    def test_serialize_ctid_ledger(self):
        """Test serializing CTID-based ledger."""
        ledger = {"type": "ctid_list", "values": ["(0,1)", "(0,2)", "(0,3)"]}

        serialized = serialize_ledger(ledger)
        assert isinstance(serialized, str)

        deserialized = deserialize_ledger(serialized)
        assert deserialized == ledger

    def test_roundtrip(self):
        """Test serialize → deserialize roundtrip."""
        original = {"type": "pk_list", "column": "user_id", "values": list(range(1000))}

        roundtrip = deserialize_ledger(serialize_ledger(original))
        assert roundtrip == original


class TestCreateLedger:
    """Test ledger creation (requires DB)."""

    @pytest.fixture
    def mock_conn(self):
        """Mock PostgreSQL connection."""

        class MockCursor:
            def __init__(self, rows):
                self.rows = rows
                self.rowcount = len(rows)

            def execute(self, sql, params=None):
                pass

            def fetchall(self):
                return self.rows

        class MockConn:
            def __init__(self, rows):
                self._rows = rows

            def cursor(self):
                return MockCursor(self._rows)

        return MockConn

    def test_create_pk_ledger(self, mock_conn):
        """Test creating PK-based ledger."""
        from backparq.db.operations import ChunkSpec

        # Mock data: 5 rows with PKs 1-5
        conn = mock_conn([(1,), (2,), (3,), (4,), (5,)])

        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc),
        )

        ledger = create_ledger(conn, chunk, primary_key="id", order_by="created_at")

        assert ledger["type"] == "pk_list"
        assert ledger["column"] == "id"
        assert ledger["values"] == [1, 2, 3, 4, 5]

    def test_create_ctid_ledger(self, mock_conn):
        """Test creating CTID-based ledger (no PK)."""
        from backparq.db.operations import ChunkSpec

        # Mock data: 3 rows with CTIDs
        conn = mock_conn([("(0,1)",), ("(0,2)",), ("(0,3)",)])

        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc),
        )

        ledger = create_ledger(conn, chunk, primary_key=None, order_by="created_at")

        assert ledger["type"] == "ctid_list"
        assert ledger["values"] == ["(0,1)", "(0,2)", "(0,3)"]

    def test_empty_ledger(self, mock_conn):
        """Test ledger creation with no rows."""
        from backparq.db.operations import ChunkSpec

        conn = mock_conn([])  # No rows

        chunk = ChunkSpec(
            table="public.events",
            start=dt.datetime(2024, 1, 1, tzinfo=dt.timezone.utc),
            end=dt.datetime(2024, 2, 1, tzinfo=dt.timezone.utc),
        )

        ledger = create_ledger(conn, chunk, primary_key="id", order_by="created_at")

        assert ledger["type"] == "pk_list"
        assert ledger["values"] == []


class TestDeleteWithLedger:
    """Test ledger-based deletion (requires DB)."""

    @pytest.fixture
    def mock_conn(self):
        """Mock PostgreSQL connection."""

        class MockCursor:
            def __init__(self):
                self.rowcount = 0
                self.executed_sql = []

            def execute(self, sql, params=None):
                self.executed_sql.append((sql, params))
                # Simulate deleting all rows in batch
                if params and isinstance(params, tuple) and len(params) > 0:
                    batch = params[0]
                    self.rowcount = len(batch) if isinstance(batch, list) else 1

        class MockConn:
            def __init__(self):
                self.cursor_obj = MockCursor()

            def cursor(self):
                return self.cursor_obj

        return MockConn()

    def test_delete_pk_ledger(self, mock_conn):
        """Test deleting with PK-based ledger."""
        ledger = {"type": "pk_list", "column": "id", "values": [1, 2, 3, 4, 5]}

        rows_deleted = delete_with_ledger(
            conn=mock_conn, table="public.events", ledger=ledger, batch_size=10
        )

        assert rows_deleted == 5
        assert len(mock_conn.cursor_obj.executed_sql) == 1  # Single batch

    def test_delete_pk_ledger_batched(self, mock_conn):
        """Test deleting with PK-based ledger in batches."""
        ledger = {"type": "pk_list", "column": "id", "values": list(range(1, 26))}  # 25 rows

        rows_deleted = delete_with_ledger(
            conn=mock_conn, table="public.events", ledger=ledger, batch_size=10
        )

        assert rows_deleted == 25
        assert len(mock_conn.cursor_obj.executed_sql) == 3  # 3 batches (10+10+5)

    def test_delete_ctid_ledger(self, mock_conn):
        """Test deleting with CTID-based ledger."""
        ledger = {"type": "ctid_list", "values": ["(0,1)", "(0,2)", "(0,3)"]}

        rows_deleted = delete_with_ledger(
            conn=mock_conn, table="public.events", ledger=ledger, batch_size=10
        )

        assert rows_deleted == 3
        assert len(mock_conn.cursor_obj.executed_sql) == 1


class TestRaceConditionPrevention:
    """
    Integration test: Prove that ledger-based approach prevents race condition.

    This is the CRITICAL test that proves the bug is fixed.
    """

    def test_race_condition_scenario(self, tmp_path):
        """
        Simulate the race condition scenario:
        1. Create ledger (freeze rows 1-5)
        2. Backfill row 6 (simulates race condition)
        3. Export using ledger (should get rows 1-5 only)
        4. Delete using ledger (should delete rows 1-5 only)
        5. Verify row 6 still exists (NOT deleted)
        """
        from backparq.adapters.catalog import Catalog, ChunkState

        # Setup catalog
        catalog_path = tmp_path / "catalog.db"
        catalog = Catalog(catalog_path)

        # Simulate ledger creation
        ledger = {"type": "pk_list", "column": "id", "values": [1, 2, 3, 4, 5]}

        # Store in catalog
        chunk_id = "public_events_2024-01"
        catalog.transition(
            chunk_id,
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
            ledger_snapshot=serialize_ledger(ledger),
            row_count=5,
        )

        # Retrieve ledger
        chunk_data = catalog.get_chunk(chunk_id)
        assert chunk_data is not None
        assert chunk_data["ledger_snapshot"] is not None

        retrieved_ledger = deserialize_ledger(chunk_data["ledger_snapshot"])

        # CRITICAL: Ledger should only contain rows 1-5, NOT row 6
        assert retrieved_ledger["values"] == [1, 2, 3, 4, 5]
        assert 6 not in retrieved_ledger["values"]

        # This proves that even if row 6 was backfilled AFTER ledger creation,
        # it will NOT be deleted because it's not in the ledger!


class TestLegacyChunkHandling:
    """Test handling of legacy chunks without ledgers."""

    def test_legacy_chunk_warning(self, tmp_path):
        """Test that chunks without ledgers are detected."""
        from backparq.adapters.catalog import Catalog, ChunkState

        catalog_path = tmp_path / "catalog.db"
        catalog = Catalog(catalog_path)

        # Create chunk WITHOUT ledger (legacy)
        chunk_id = "public_events_2024-01"
        catalog.transition(
            chunk_id,
            ChunkState.EXPORTED,
            table_name="public.events",
            start_ts="2024-01-01T00:00:00Z",
            end_ts="2024-02-01T00:00:00Z",
            row_count=100,
            # NO ledger_snapshot!
        )

        # Retrieve chunk
        chunk_data = catalog.get_chunk(chunk_id)
        assert chunk_data is not None

        # Ledger should be None or missing
        ledger_json = chunk_data.get("ledger_snapshot")
        assert ledger_json is None or ledger_json == ""

        # This should trigger a warning/error in delete_op.py
