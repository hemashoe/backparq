"""Backup listing."""

from __future__ import annotations

import json
import logging
from collections import defaultdict

from rich.table import Table

from backparq.config import BackparqConfig
from backparq.utils.console import (
    console,
    format_count,
    format_size,
    print_header,
    print_success,
    print_warning,
)
from backparq.storage.s3 import create_client as s3_client_from_config

logger = logging.getLogger(__name__)


def check_backups(config: BackparqConfig, output_json: bool = False) -> dict:
    result = {"backups": [], "summary": {}}

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
    info = {"key": key, "table": None, "year": None, "month": None, "mode": None}
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

    filename = parts[-1] if parts else ""
    if filename.endswith(".parquet"):
        name = filename.rsplit("_", 1)[0]
        info["table"] = name.replace("_", ".", 1) if "_" in name else name

    return info
