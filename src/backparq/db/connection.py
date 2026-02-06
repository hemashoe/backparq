"""Database connection and pooling."""

from __future__ import annotations

import logging
import threading
from contextlib import contextmanager
from typing import Optional

import psycopg2
from psycopg2 import pool
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import DatabaseConfig

logger = logging.getLogger(__name__)


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def connect(config: DatabaseConfig):
    """Create a PostgreSQL connection with retry logic."""
    logger.debug(f"Connecting to {config.host}:{config.port}/{config.name}")
    conn = psycopg2.connect(config.dsn())
    conn.autocommit = False
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
    """Thread-safe connection pool."""

    def __init__(self, config: DatabaseConfig, minconn: int = 2, maxconn: int = 10):
        self.config = config
        self.minconn = minconn
        self.maxconn = maxconn
        self._pool: Optional[pool.ThreadedConnectionPool] = None
        self._lock = threading.Lock()

    def _ensure_pool(self) -> pool.ThreadedConnectionPool:
        if self._pool is None:
            with self._lock:
                if self._pool is None:
                    logger.info(f"Creating pool: min={self.minconn}, max={self.maxconn}")
                    self._pool = pool.ThreadedConnectionPool(
                        self.minconn,
                        self.maxconn,
                        host=self.config.host,
                        port=self.config.port,
                        dbname=self.config.name,
                        user=self.config.user,
                        password=self.config.password,
                        connect_timeout=self.config.connect_timeout,
                    )
        return self._pool

    @contextmanager
    def connection(self):
        p = self._ensure_pool()
        conn = p.getconn()
        try:
            yield conn
            conn.commit()
        except Exception:
            conn.rollback()
            raise
        finally:
            p.putconn(conn)

    @contextmanager
    def autocommit_connection(self):
        """Connection with autocommit for DDL operations like VACUUM."""
        p = self._ensure_pool()
        conn = p.getconn()
        old = conn.autocommit
        try:
            conn.autocommit = True
            yield conn
        finally:
            conn.autocommit = old
            p.putconn(conn)

    def close(self) -> None:
        if self._pool:
            self._pool.closeall()
            self._pool = None
            logger.info("Pool closed")

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()
