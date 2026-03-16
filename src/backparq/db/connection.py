from __future__ import annotations

import logging
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional

import psycopg
from psycopg_pool import ConnectionPool as PsycopgPool
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import DatabaseConfig

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def connect(config: DatabaseConfig) -> psycopg.Connection:
    """Create a PostgreSQL connection with retry logic."""
    logger.debug(f"Connecting to {config.host}:{config.port}/{config.name}")
    # psycopg3 uses conninfo string or kwargs.
    # config.dsn() likely returns a libpq-style string which psycopg3 supports.
    conn = psycopg.connect(config.dsn(), autocommit=False)
    return conn


def test_connection(config: DatabaseConfig) -> None:
    """Verify database connectivity."""
    conn = connect(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        logger.info("Database connection OK")
    finally:
        conn.close()


class ConnectionPool:
    """Thread-safe connection pool using psycopg_pool."""

    def __init__(self, config: DatabaseConfig, minconn: int = 2, maxconn: int = 10):
        self.config = config
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: Optional[PsycopgPool] = None
        self._lock = threading.Lock()

    def _ensure_pool(self) -> PsycopgPool:
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    logger.info(f"Creating pool: min={self.minconn}, max={self.maxconn}")
                    # psycopg_pool.ConnectionPool takes conninfo as first arg
                    self._pool = PsycopgPool(
                        self.config.dsn(),
                        min_size=self.minconn,
                        max_size=self.maxconn,
                        timeout=self.config.connect_timeout,
                        open=True,  # Open immediately
                    )
        return self._pool

    @contextmanager
    def connection(self) -> Iterator[psycopg.Connection]:
        """Get a READ COMMITTED connection from the pool."""
        pool = self._ensure_pool()
        with pool.connection() as conn:
            try:
                conn.isolation_level = psycopg.IsolationLevel.READ_COMMITTED
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    @contextmanager
    def autocommit_connection(self) -> Iterator[psycopg.Connection]:
        """Connection with autocommit for DDL operations like VACUUM."""
        pool = self._ensure_pool()
        with pool.connection() as conn:
            old = conn.autocommit
            try:
                conn.autocommit = True
                yield conn
            finally:
                conn.autocommit = old

    def close(self) -> None:
        if self._pool:
            self._pool.close()
            self._pool = None
            logger.info("Pool closed")

    def __enter__(self) -> ConnectionPool:
        return self

    def __exit__(self, *args: Any) -> None:
        self.close()
