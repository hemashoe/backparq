from __future__ import annotations

import datetime as dt
import logging

from rich.table import Table

from backparq.config import BackparqConfig
from backparq.storage.s3 import create_client as s3_client_from_config
from backparq.utils.console import (
    console,
    create_progress,
    format_size,
    print_header,
    print_success,
    print_warning,
)

logger = logging.getLogger(__name__)


def prune_backups(config: BackparqConfig, dry_run: bool = False) -> dict:
    result = {"deleted": [], "summary": {"files_deleted": 0, "bytes_freed": 0}}

    if not config.archive.retention.enabled:
        print_warning("Retention disabled")
        return result

    if not config.s3.bucket:
        print_warning("No S3 bucket configured")
        return result

    print_header("BACKPARQ PRUNE")
    retention_days = config.archive.retention.total_days
    console.print(f"Retention: {retention_days} days")
    console.print(f"Dry Run: {'yes' if dry_run else 'no'}")
    console.print()

    s3 = s3_client_from_config(config.s3)
    prefix = f"{config.s3.prefix}/"
    cutoff = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=retention_days)

    console.print(f"Cutoff: {cutoff.strftime('%Y-%m-%d')}")
    console.print()

    to_delete = []
    paginator = s3.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=config.s3.bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if not key.endswith(".parquet"):
                continue

            year, month = _parse_year_month(key)
            if year and month:
                file_date = dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)
                if file_date < cutoff:
                    to_delete.append({"key": key, "size": obj["Size"], "date": file_date})

    if not to_delete:
        print_success("No files to prune")
        return result

    table = Table(show_header=True, header_style="bold")
    table.add_column("File")
    table.add_column("Date")
    table.add_column("Size", justify="right")

    for item in to_delete[:20]:
        table.add_row(
            item["key"].split("/")[-1], item["date"].strftime("%Y-%m"), format_size(item["size"])
        )

    if len(to_delete) > 20:
        table.add_row("...", f"+{len(to_delete) - 20} more", "")

    console.print(table)
    console.print()

    total_size = sum(i["size"] for i in to_delete)
    console.print(f"Files to delete: {len(to_delete)} ({format_size(total_size)})")
    console.print()

    if dry_run:
        print_warning("DRY RUN: No files deleted")
        return result

    with create_progress() as progress:
        task = progress.add_task("Deleting", total=len(to_delete))
        for item in to_delete:
            try:
                s3.delete_object(Bucket=config.s3.bucket, Key=item["key"])
                result["deleted"].append(item["key"])
                result["summary"]["files_deleted"] += 1
                result["summary"]["bytes_freed"] += item["size"]
            except Exception as e:
                logger.error(f"Delete failed {item['key']}: {e}")
            progress.advance(task)

    print_success(
        f"Deleted {result['summary']['files_deleted']} files ({format_size(result['summary']['bytes_freed'])})"
    )
    return result


def _parse_year_month(key: str) -> tuple[int, int]:
    year = month = 0
    for p in key.split("/"):
        if p.startswith("year="):
            try:
                year = int(p.split("=")[1])
            except (ValueError, IndexError):
                pass
        if p.startswith("month="):
            try:
                month = int(p.split("=")[1])
            except (ValueError, IndexError):
                pass
    return year, month
