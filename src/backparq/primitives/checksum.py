"""SHA256 checksum computation - pure function."""

from __future__ import annotations

import hashlib
from pathlib import Path


def compute_sha256(path: Path, buf_size: int = 8 * 1024 * 1024) -> str:
    """
    Compute SHA256 hash of file.

    Args:
        path: Path to file
        buf_size: Buffer size for reading (default 8MB)

    Returns:
        Hexadecimal SHA256 hash string
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        while chunk := f.read(buf_size):
            h.update(chunk)
    return h.hexdigest()
