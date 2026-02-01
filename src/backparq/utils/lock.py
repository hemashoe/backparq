"""File-based lock to prevent concurrent runs."""

from __future__ import annotations

import json
import logging
import os
import socket
import time
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

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
                return json.load(f)
        except (json.JSONDecodeError, OSError):
            return None

    def _write_lock(self) -> None:
        self.base_dir.mkdir(parents=True, exist_ok=True)
        data = {
            "pid": os.getpid(),
            "hostname": socket.gethostname(),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
        with open(self.lock_path, "w") as f:
            json.dump(data, f)

    def _is_stale(self, info: dict) -> bool:
        try:
            os.kill(info.get("pid", 0), 0)
        except OSError:
            return True
        try:
            started = datetime.fromisoformat(info["started_at"])
            age = (datetime.now(timezone.utc) - started).total_seconds()
            return age > self.stale_timeout
        except (ValueError, KeyError):
            return False

    def acquire(self, timeout: int = 0) -> bool:
        start = time.time()
        while True:
            existing = self._read_lock()
            if existing is None or self._is_stale(existing):
                self._write_lock()
                self._acquired = True
                logger.info(f"Lock acquired: {self.lock_path}")
                return True
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
    def hold(self, timeout: int = 0):
        self.acquire(timeout=timeout)
        try:
            yield
        finally:
            self.release()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *args):
        self.release()
