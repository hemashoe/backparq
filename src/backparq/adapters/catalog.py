from __future__ import annotations

import datetime as dt
import logging
import sqlite3
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from enum import Enum
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)


class ChunkState(int, Enum):
    """Lifecycle states for a chunk of data."""

    IN_DB = 1  # Data exists only in PostgreSQL
    EXPORTED = 2  # Parquet file created locally
    UPLOADED = 3  # Verified on S3
    OFFLOADED = 4  # Deleted from PostgreSQL (only in S3)
    PRUNED = 5  # Deleted from S3 (retention expired)


class RunStatus(str, Enum):
    """Status of an archive/restore run."""

    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"


# Schema version for migrations
SCHEMA_VERSION = 1

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS schema_version (
    version INTEGER PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS chunks (
    id TEXT PRIMARY KEY,
    table_name TEXT NOT NULL,
    start_ts TEXT NOT NULL,
    end_ts TEXT NOT NULL,
    state INTEGER NOT NULL,
    local_path TEXT,
    s3_key TEXT,
    sha256 TEXT,
    row_count INTEGER,
    byte_size INTEGER,
    ledger_snapshot TEXT,
    watermark_id TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_chunks_table ON chunks(table_name);
CREATE INDEX IF NOT EXISTS idx_chunks_state ON chunks(state);
CREATE INDEX IF NOT EXISTS idx_chunks_updated ON chunks(updated_at);

CREATE TABLE IF NOT EXISTS runs (
    id TEXT PRIMARY KEY,
    mode TEXT NOT NULL,
    started_at TEXT NOT NULL,
    finished_at TEXT,
    status TEXT NOT NULL,
    config_hash TEXT,
    error TEXT
);

CREATE INDEX IF NOT EXISTS idx_runs_started ON runs(started_at);
CREATE INDEX IF NOT EXISTS idx_runs_status ON runs(status);
"""


class Catalog:
    """SQLite-based state store for chunk lifecycle tracking."""

    def __init__(self, db_path: Path):
        """
        Initialize catalog.

        Args:
            db_path: Path to SQLite database file
        """
        self.db_path = Path(db_path)
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def _init_schema(self) -> None:
        """Initialize database schema if needed."""
        with self._connection() as conn:
            # Enable WAL mode for concurrent readers/writers without blocking.
            conn.execute("PRAGMA journal_mode=WAL")
            conn.executescript(SCHEMA_SQL)

            # Check/set schema version
            cursor = conn.execute("SELECT version FROM schema_version")
            row = cursor.fetchone()
            if row is None:
                conn.execute("INSERT INTO schema_version (version) VALUES (?)", (SCHEMA_VERSION,))
                logger.debug(f"Initialized catalog schema v{SCHEMA_VERSION}")
            else:
                version = row[0]
                if version != SCHEMA_VERSION:
                    logger.warning(
                        f"Catalog schema version mismatch: {version} != {SCHEMA_VERSION}"
                    )

            # Migration: Add ledger_snapshot column if missing
            cursor = conn.execute(
                "SELECT COUNT(*) FROM pragma_table_info('chunks') WHERE name='ledger_snapshot'"
            )
            if cursor.fetchone()[0] == 0:
                conn.execute("ALTER TABLE chunks ADD COLUMN ledger_snapshot TEXT")
                logger.info("Migrated catalog: added ledger_snapshot column")

            # Migration: Add watermark_id column if missing
            cursor = conn.execute(
                "SELECT COUNT(*) FROM pragma_table_info('chunks') WHERE name='watermark_id'"
            )
            if cursor.fetchone()[0] == 0:
                conn.execute("ALTER TABLE chunks ADD COLUMN watermark_id TEXT")
                logger.info("Migrated catalog: added watermark_id column")

    @contextmanager
    def _connection(self) -> Iterator[sqlite3.Connection]:
        """Get a connection with row factory."""
        conn = sqlite3.connect(str(self.db_path))
        conn.row_factory = sqlite3.Row
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()

    def get_state(self, chunk_id: str) -> Optional[ChunkState]:
        """
        Get current state of a chunk.

        Args:
            chunk_id: Unique chunk identifier

        Returns:
            ChunkState or None if chunk not found
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT state FROM chunks WHERE id = ?", (chunk_id,))
            row = cursor.fetchone()
            return ChunkState(int(row["state"])) if row else None

    def get_chunk(self, chunk_id: str) -> Optional[dict[str, Any]]:
        """
        Get full chunk record.

        Args:
            chunk_id: Unique chunk identifier

        Returns:
            Dict with chunk data or None if not found
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT * FROM chunks WHERE id = ?", (chunk_id,))
            row = cursor.fetchone()
            return dict(row) if row else None

    def transition(
        self,
        chunk_id: str,
        new_state: ChunkState,
        **metadata: Any,
    ) -> None:
        """
        Transition chunk to new state (atomic).

        Args:
            chunk_id: Unique chunk identifier
            new_state: Target state
            **metadata: Additional fields to update (sha256, local_path, s3_key, row_count, byte_size)
        """
        now = dt.datetime.now(dt.timezone.utc).isoformat()

        with self._connection() as conn:
            cursor = conn.execute("SELECT state FROM chunks WHERE id = ?", (chunk_id,))
            row = cursor.fetchone()

            if row is None:
                # Create new chunk record
                conn.execute(
                    """
                    INSERT INTO chunks (
                        id, table_name, start_ts, end_ts, state,
                        local_path, s3_key, sha256, row_count, byte_size, ledger_snapshot, watermark_id,
                        created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        chunk_id,
                        metadata.get("table_name", ""),
                        metadata.get("start_ts", ""),
                        metadata.get("end_ts", ""),
                        new_state.value,
                        metadata.get("local_path"),
                        metadata.get("s3_key"),
                        metadata.get("sha256"),
                        metadata.get("row_count"),
                        metadata.get("byte_size"),
                        metadata.get("ledger_snapshot"),
                        metadata.get("watermark_id"),
                        now,
                        now,
                    ),
                )
                logger.debug(f"Created chunk {chunk_id} in state {new_state.value}")
            else:
                # Update existing chunk
                update_fields = ["state = ?", "updated_at = ?"]
                values: list[Any] = [new_state.value, now]

                for key in [
                    "local_path",
                    "s3_key",
                    "sha256",
                    "row_count",
                    "byte_size",
                    "ledger_snapshot",
                    "watermark_id",
                ]:
                    if key in metadata:
                        update_fields.append(f"{key} = ?")
                        values.append(metadata[key])

                values.append(chunk_id)
                stmt = f"UPDATE chunks SET {', '.join(update_fields)} WHERE id = ?"
                conn.execute(stmt, values)
                logger.debug(f"Transitioned chunk {chunk_id} to {new_state.value}")

    def list_chunks(
        self,
        table_name: Optional[str] = None,
        state: Optional[ChunkState] = None,
    ) -> list[dict[str, Any]]:
        """
        List chunks matching criteria.

        Args:
            table_name: Filter by table name (exact match)
            state: Filter by state

        Returns:
            List of chunk records
        """
        with self._connection() as conn:
            query = "SELECT * FROM chunks WHERE 1=1"
            params: list[Any] = []

            if table_name:
                query += " AND table_name = ?"
                params.append(table_name)

            if state:
                query += " AND state = ?"
                params.append(state.value)

            query += " ORDER BY table_name, start_ts"

            cursor = conn.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def start_run(self, mode: str, config_hash: Optional[str] = None) -> str:
        """
        Start a new run.

        Args:
            mode: "backup", "offload", or "restore"
            config_hash: Optional SHA256 of config used

        Returns:
            Run ID (timestamp + UUID suffix — guaranteed unique)
        """
        run_id = (
            dt.datetime.now(dt.timezone.utc).strftime("%Y%m%d_%H%M%S")
            + "_"
            + uuid.uuid4().hex[:6]
        )
        now = dt.datetime.now(dt.timezone.utc).isoformat()

        with self._connection() as conn:
            conn.execute(
                """
                INSERT INTO runs (id, mode, started_at, status, config_hash)
                VALUES (?, ?, ?, ?, ?)
                """,
                (run_id, mode, now, RunStatus.RUNNING.value, config_hash),
            )

        logger.info(f"Started run {run_id} (mode={mode})")
        return run_id

    def finish_run(
        self, run_id: str, status: RunStatus = RunStatus.COMPLETED, error: Optional[str] = None
    ) -> None:
        """
        Mark run as finished.

        Args:
            run_id: Run identifier
            status: Final status
            error: Optional error message if failed
        """
        now = dt.datetime.now(dt.timezone.utc).isoformat()

        with self._connection() as conn:
            conn.execute(
                "UPDATE runs SET finished_at = ?, status = ?, error = ? WHERE id = ?",
                (now, status.value, error, run_id),
            )

        logger.info(f"Finished run {run_id} (status={status.value})")

    def get_history(self, limit: int = 20) -> list[dict[str, Any]]:
        """
        Get recent run history.

        Args:
            limit: Maximum number of runs to return

        Returns:
            List of run records, most recent first
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT * FROM runs ORDER BY started_at DESC LIMIT ?", (limit,))
            return [dict(row) for row in cursor.fetchall()]

    def get_stats(self) -> dict[str, Any]:
        """
        Get catalog statistics.

        Returns:
            Dict with counts by state, total chunks, etc.
        """
        with self._connection() as conn:
            cursor = conn.execute("SELECT state, COUNT(*) as count FROM chunks GROUP BY state")
            by_state = {row["state"]: row["count"] for row in cursor.fetchall()}

            cursor = conn.execute("SELECT COUNT(*) as total FROM chunks")
            total = cursor.fetchone()["total"]

            cursor = conn.execute("SELECT COUNT(*) as total FROM runs")
            total_runs = cursor.fetchone()["total"]

            return {
                "total_chunks": total,
                "total_runs": total_runs,
                "chunks_by_state": by_state,
            }

    def close(self) -> None:
        """Close catalog (no-op for context manager compatibility)."""
        pass

    def __enter__(self) -> Catalog:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
