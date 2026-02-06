from __future__ import annotations

import json
import logging
import os
import socket
import time
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

LOCK_FILENAME = ".backparq.lock"
STALE_LOCK_SECONDS = 3600


class LockError(Exception):
    pass


class Lock:
    def __init__(self, base_dir: Path, stale_timeout: int = STALE_LOCK_SECONDS):
        self.base_dir = Path(base_dir)
        self.lock_path = self.base_dir / LOCK_FILENAME
        self.stale_timeout = stale_timeout
        self._acquired = False

    def _read_lock(self) -> Optional[dict]:
        if not self.lock_path.exists():
            return None
        try:
            with open(self.lock_path) as f:
                data = json.load(f)
                if isinstance(data, dict):
                    return data
                return None
        except (json.JSONDecodeError, OSError):
            return None

    def _write_lock_atomic(self) -> bool:
        """Write lock file atomically using O_CREAT|O_EXCL.

        Returns True if lock was successfully created, False if it already exists.
        This is atomic at the filesystem level, preventing race conditions.
        """
        self.base_dir.mkdir(parents=True, exist_ok=True)
        data = {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            # O_CREAT | O_EXCL: Create file only if it doesn't exist (atomic)
            # O_WRONLY: Open for writing only
            fd = os.open(str(self.lock_path), os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o644)
            try:
                os.write(fd, json.dumps(data).encode())
            finally:
                os.close(fd)
            return True
        except FileExistsError:
            return False
        except OSError as e:
            logger.warning(f"Failed to create lock file: {e}")
            return False

    def _remove_stale_lock(self) -> bool:
        """Remove a stale lock file. Returns True if removed."""
        try:
            self.lock_path.unlink()
            return True
        except OSError:
            return False

    def _is_stale(self, info: dict) -> bool:
        """Check if lock is stale based on PID (local) or timestamp (remote)."""
        pid = info.get("pid")
        hostname = info.get("hostname")
        started_at_str = info.get("started_at")

        # If lock file is corrupted/empty
        if not pid or not hostname or not started_at_str:
            return True

        current_hostname = socket.gethostname()

        # 1. Local Process Check
        if hostname == current_hostname:
            try:
                os.kill(pid, 0)
                # Process exists, not stale
                return False
            except OSError:
                # Process dead
                logger.warning(f"Found stale local lock (PID {pid} dead), claiming it.")
                return True

        # 2. Remote Process Check (Time-based)
        try:
            started = datetime.fromisoformat(started_at_str)
            age = (datetime.now(timezone.utc) - started).total_seconds()
            if age > self.stale_timeout:
                logger.warning(
                    f"Found stale remote lock from {hostname} (age {age}s > {self.stale_timeout}s), claiming it."
                )
                return True
            return False
        except (ValueError, TypeError):
            # Invalid date format
            return True

    def acquire(self, timeout: int = 0) -> bool:
        """Acquire the lock with atomic file creation.

        Uses O_CREAT|O_EXCL for race-free lock acquisition.
        If a stale lock exists, removes it and retries.
        """
        start = time.time()
        while True:
            # Try atomic creation first
            if self._write_lock_atomic():
                self._acquired = True
                logger.info(f"Lock acquired: {self.lock_path}")
                return True

            # Lock file exists - check if it's stale
            existing = self._read_lock()
            if existing is None:
                # File was removed between our atomic create and read - retry
                continue

            if self._is_stale(existing):
                # Remove stale lock and retry
                if self._remove_stale_lock():
                    continue
                # Another process may have taken the lock - retry
                continue

            # Lock is held by another active process
            if timeout > 0 and (time.time() - start) < timeout:
                time.sleep(1)
                continue

            raise LockError(
                f"Cannot acquire lock. Process {existing.get('pid')} running since {existing.get('started_at')}"
            )

    def release(self) -> None:
        if self._acquired and self.lock_path.exists():
            try:
                self.lock_path.unlink()
            except OSError:
                pass
            self._acquired = False
            logger.info("Lock released")

    @contextmanager
    def hold(self, timeout: int = 0) -> Iterator[None]:
        self.acquire(timeout=timeout)
        try:
            yield
        finally:
            self.release()

    def __enter__(self) -> Lock:
        self.acquire()
        return self

    def __exit__(self, *args: Any) -> None:
        self.release()
