"""Backup listing and inventory."""

from __future__ import annotations

import json
import logging
from collections import defaultdict
from typing import Any

from rich.table import Table

from backparq.config import BackparqConfig
from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.utils.console import (
    console,
    format_count,
    format_size,
    print_header,
    print_success,
    print_warning,
)

logger = logging.getLogger(__name__)


def check_backups(config: BackparqConfig, output_json: bool = False) -> dict:
    """List and summarize backups in S3."""
    result: dict[str, Any] = {"backups": [], "summary": {}}

    if not config.s3.bucket:
        print_warning("No S3 bucket configured")
        return result

    if not output_json:
        print_header("BACKPARQ CHECK")
        console.print(f"Bucket: s3://{config.s3.bucket}/{config.s3.prefix}")
        console.print()

    s3 = s3_client_from_config(config.s3)
    prefix = f"{config.s3.prefix}/"
    paginator = s3.get_paginator("list_objects_v2")

    backups = []
    total_size = 0

    for page in paginator.paginate(Bucket=config.s3.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue

            size = obj["Size"]
            total_size += size

            info = _parse_backup_key(key, config.s3.prefix)
            info["size"] = size
            info["last_modified"] = obj["LastModified"].isoformat()

            try:
                head = s3.head_object(Bucket=config.s3.bucket, Key=key)
                meta = head.get("Metadata", {})
                info["rows"] = int(meta.get("rows", 0))
                info["verified"] = bool(meta.get("sha256"))
            except Exception:
                info["rows"] = 0
                info["verified"] = False

            backups.append(info)

    if not backups:
        print_warning("No backups found")
        return result

    by_table = defaultdict(list)
    for b in backups:
        by_table[b.get("table", "unknown")].append(b)

    result["backups"] = backups
    result["summary"] = {
        "total_files": len(backups),
        "total_size": total_size,
        "tables": len(by_table),
    }

    if output_json:
        print(json.dumps(result, indent=2, default=str))
        return result

    table = Table(show_header=True, header_style="bold")
    table.add_column("Table")
    table.add_column("Files", justify="right")
    table.add_column("Rows", justify="right")
    table.add_column("Size", justify="right")
    table.add_column("Verified")

    for tbl, items in sorted(by_table.items()):
        total_rows = sum(i.get("rows", 0) for i in items)
        total_sz = sum(i["size"] for i in items)
        verified = all(i.get("verified") for i in items)
        table.add_row(
            tbl,
            str(len(items)),
            format_count(total_rows),
            format_size(total_sz),
            "[green]Yes[/green]" if verified else "[yellow]No[/yellow]",
        )

    console.print(table)
    console.print()
    print_success(
        f"Found {len(backups)} files ({format_size(total_size)}) across {len(by_table)} tables"
    )

    return result


def _parse_backup_key(key: str, prefix: str) -> dict:
    """Parse an S3 key into structured backup info.

    The S3 key format is:
        {prefix}/{mode}/{table_name}/year={Y}/month={M}/{filename}.parquet

    The table name is extracted from the directory path (not the filename)
    to avoid issues with underscores in table names being confused with
    schema.table separators.
    """
    info: dict[str, Any] = {"key": key, "table": None, "year": None, "month": None, "mode": None}
    path = key[len(prefix) :].lstrip("/")
    parts = path.split("/")

    if "backups" in parts:
        info["mode"] = "backup"
    elif "archive" in parts:
        info["mode"] = "offload"

    for p in parts:
        if p.startswith("year="):
            try:
                info["year"] = int(p.split("=")[1])
            except (ValueError, IndexError):
                pass
        elif p.startswith("month="):
            try:
                info["month"] = int(p.split("=")[1])
            except (ValueError, IndexError):
                pass

    # Extract table name from directory structure, not filename.
    # Path format: {mode}/{table_name}/year=.../month=.../file.parquet
    # The table directory is the segment after the mode directory.
    mode_dirs = ("archive", "backups")
    for i, part in enumerate(parts):
        if part in mode_dirs and i + 1 < len(parts):
            candidate = parts[i + 1]
            if not candidate.startswith("year=") and not candidate.startswith("month="):
                info["table"] = candidate
                break

    return info
