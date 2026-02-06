"""Backparq - PostgreSQL to Parquet archiver with S3 support."""

from backparq.archive import archive_tables
from backparq.config import BackparqConfig
from backparq.prune import prune_backups
from backparq.restore import restore_tables
from backparq.verify import verify_archives

__all__ = [
    "BackparqConfig",
    "archive_tables",
    "restore_tables",
    "prune_backups",
    "verify_archives",
]

__version__ = "0.4.0"
