from __future__ import annotations


class BackparqError(Exception):
    """Base exception for all backparq errors."""

    pass


class ConfigError(BackparqError, ValueError):
    """Configuration is invalid or missing."""

    pass


class ArchiveError(BackparqError):
    """Error during archive operation."""

    pass


class RestoreError(BackparqError):
    """Error during restore operation."""

    pass


class ChecksumError(BackparqError):
    """Checksum verification failed."""

    pass


class ConnectionError(BackparqError):
    """Database or S3 connection failed."""

    pass


class TableNotFoundError(BackparqError):
    """Table does not exist in database."""

    pass
