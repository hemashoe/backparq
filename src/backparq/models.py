from __future__ import annotations

import datetime as dt
from dataclasses import dataclass, field


@dataclass
class ArchiveResult:
    """Result of an archive operation."""

    tables_processed: int = 0
    chunks_archived: int = 0
    rows_archived: int = 0
    bytes_uploaded: int = 0
    duration_seconds: float = 0.0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return len(self.errors) == 0

    @property
    def rows_per_second(self) -> float:
        if self.duration_seconds > 0:
            return self.rows_archived / self.duration_seconds
        return 0.0

    def to_dict(self) -> dict:
        return {
            "tables_processed": self.tables_processed,
            "chunks_archived": self.chunks_archived,
            "rows_archived": self.rows_archived,
            "bytes_uploaded": self.bytes_uploaded,
            "duration_seconds": round(self.duration_seconds, 2),
            "throughput_rows_per_sec": round(self.rows_per_second, 1),
            "success": self.success,
            "errors": self.errors,
        }


@dataclass
class VerifyResult:
    """Result of a verify operation."""

    files_checked: int = 0
    files_valid: int = 0
    files_corrupted: int = 0
    files_repaired: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def success(self) -> bool:
        return self.files_corrupted == 0 or self.files_corrupted == self.files_repaired

    def to_dict(self) -> dict:
        return {
            "files_checked": self.files_checked,
            "files_valid": self.files_valid,
            "files_corrupted": self.files_corrupted,
            "files_repaired": self.files_repaired,
            "success": self.success,
            "errors": self.errors,
        }


@dataclass
class TableProgress:
    """Progress tracking for a single table."""

    status: str = "pending"
    chunks_total: int = 0
    chunks_complete: int = 0
    rows_archived: int = 0
    bytes_uploaded: int = 0

    def to_dict(self) -> dict:
        return {
            "status": self.status,
            "chunks_total": self.chunks_total,
            "chunks_complete": self.chunks_complete,
            "rows_archived": self.rows_archived,
            "bytes_uploaded": self.bytes_uploaded,
        }


@dataclass
class RunProgress:
    """Progress tracking for entire archive run."""

    run_id: str
    started_at: dt.datetime
    tables: dict[str, TableProgress] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "run_id": self.run_id,
            "started_at": self.started_at.isoformat(),
            "tables": {k: v.to_dict() for k, v in self.tables.items()},
        }
