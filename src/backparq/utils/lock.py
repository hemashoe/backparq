from __future__ import annotations

import logging
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import psycopg
from psycopg import sql

logger = logging.getLogger(__name__)


class LockError(Exception):
    pass


class AdvisoryLock:
    """
    PostgreSQL Advisory Lock.

    Uses a 64-bit integer key or a string key (hashed to 64-bit int).
    The lock is session-level and automatically released when the connection closes.
    """

    def __init__(self, conn: psycopg.Connection, key: str | int):
        self.conn = conn
        if isinstance(key, str):
            # Hashtext returns a 32-bit int, but valid for adv lock which takes bigint
            # We use a stable hash of the string
            import zlib

            self.key = zlib.crc32(key.encode("utf-8"))
        else:
            self.key = key
        self._acquired = False

    def acquire(self) -> bool:
        """Try to acquire the lock immediately. Returns True if successful."""
        try:
            cur = self.conn.execute("SELECT pg_try_advisory_lock(%s)", (self.key,))
            result = cur.fetchone()
            if result and result[0]:
                self._acquired = True
                logger.info(f"Advisory lock {self.key} acquired")
                return True
            return False
        except Exception as e:
            logger.error(f"Failed to acquire advisory lock: {e}")
            raise LockError(f"Database error during lock acquisition: {e}") from e

    def release(self) -> None:
        """Release the lock."""
        if self._acquired:
            try:
                self.conn.execute("SELECT pg_advisory_unlock(%s)", (self.key,))
                self._acquired = False
                logger.info(f"Advisory lock {self.key} released")
            except Exception as e:
                logger.error(f"Failed to release advisory lock: {e}")
                # Don't raise, as connection close will auto-release anyway

    @contextmanager
    def hold(self) -> Iterator[None]:
        if not self.acquire():
            raise LockError(f"Could not acquire lock {self.key}")
        try:
            yield
        finally:
            self.release()
