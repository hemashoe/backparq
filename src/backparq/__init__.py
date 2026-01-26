"""Package for archiving Postgres tables to Parquet and S3."""

from backparq.archive import archive_tables
from backparq.config import BackparqConfig

__all__ = ["BackparqConfig", "archive_tables"]
