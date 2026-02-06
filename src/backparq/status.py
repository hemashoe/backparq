"""Archive status display."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any, Optional

from rich.table import Table

from backparq.config import BackparqConfig
from backparq.db import connect_pg, pg_count_rows, pg_get_min_created_at, table_exists
from backparq.storage.parquet import load_manifest
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


def show_status(
    config: BackparqConfig, table_filter: Optional[str] = None, output_json: bool = False
) -> dict:
    result: dict[str, Any] = {"tables": [], "summary": {}}

    if not output_json:
        print_header("BACKPARQ STATUS")
        console.print(f"Mode: [bold]{config.archive.mode}[/bold]")
        console.print(
            f"S3: s3://{config.s3.bucket}/{config.s3.prefix}"
            if config.s3.bucket
            else "S3: Not configured"
        )
        console.print(f"Base Dir: {config.archive.base_dir}")

        # Catalog Stats
        cat_stats = _get_catalog_stats(config)
        if cat_stats:
            console.print(
                f"Catalog: {cat_stats['total_runs']} runs, {cat_stats['total_chunks']} chunks tracked"
            )
            states = []
            for state, count in cat_stats.get("chunks_by_state", {}).items():
                state_name = {1: "IN_DB", 2: "EXPORTED", 3: "UPLOADED", 4: "OFFLOADED"}.get(
                    state, str(state)
                )
                states.append(f"{state_name}={count}")
            if states:
                console.print(f"States:  {', '.join(states)}")
        console.print()

    conn = connect_pg(config.database)
    s3 = None
    if config.s3.bucket:
        try:
            s3 = s3_client_from_config(config.s3)
        except Exception as e:
            logger.warning(f"S3 connection failed: {e}")

    tables = config.archive.tables
    if table_filter:
        tables = [t for t in tables if table_filter.lower() in t.name.lower()]

    status_table = Table(show_header=True, header_style="bold")
    status_table.add_column("Table")
    status_table.add_column("DB Pending", justify="right")
    status_table.add_column("Local", justify="right")
    status_table.add_column("S3", justify="right")
    status_table.add_column("Status")

    total_pending = 0

    try:
        for table_config in tables:
            table = table_config.name
            table_data: dict[str, Any] = {"name": table, "primary_key": table_config.primary_key}

            if not table_exists(conn, table):
                table_data["status"] = "not_found"
                result["tables"].append(table_data)
                if not output_json:
                    status_table.add_row(table, "-", "-", "-", "[red]Not Found[/red]")
                continue

            db_stats = _get_db_stats(
                conn, table, config.archive.order_by, config.archive.cutoff_exclusive
            )
            table_data["db_pending"] = db_stats["archivable_rows"]
            total_pending += db_stats["archivable_rows"]

            local_stats = _get_local_stats(config.archive.base_dir, table)
            table_data["local_chunks"] = local_stats["chunk_count"]
            table_data["local_rows"] = local_stats["total_rows"]
            table_data["local_size"] = local_stats["total_size"]

            s3_stats = {"chunk_count": 0, "total_size": 0}
            if s3:
                s3_stats = _get_s3_stats(
                    s3, config.s3.bucket, config.s3.prefix, table, config.archive.mode
                )
            table_data["s3_chunks"] = s3_stats["chunk_count"]
            table_data["s3_size"] = s3_stats["total_size"]

            if db_stats["archivable_rows"] == 0 and local_stats["chunk_count"] > 0:
                status = "[green]Up to date[/green]"
                table_data["status"] = "up_to_date"
            elif db_stats["archivable_rows"] > 0:
                status = f"[yellow]{format_count(db_stats['archivable_rows'])} pending[/yellow]"
                table_data["status"] = "pending"
            else:
                status = "[dim]Empty[/dim]"
                table_data["status"] = "empty"

            result["tables"].append(table_data)

            if not output_json:
                db_str = format_count(db_stats["archivable_rows"])
                local_str = (
                    f"{local_stats['chunk_count']} ({format_size(local_stats['total_size'])})"
                    if local_stats["chunk_count"]
                    else "0"
                )
                s3_str = (
                    f"{s3_stats['chunk_count']} ({format_size(s3_stats['total_size'])})"
                    if s3_stats["chunk_count"]
                    else "0"
                )
                status_table.add_row(table, db_str, local_str, s3_str, status)
    finally:
        conn.close()

    result["summary"] = {"total_pending": total_pending}

    if output_json:
        print(json.dumps(result, indent=2))
    else:
        console.print(status_table)
        console.print()
        if total_pending > 0:
            print_warning(f"Total pending: {format_count(total_pending)} rows")
        else:
            print_success("All tables up to date")

    return result


def _get_db_stats(conn: Any, table: str, order_by: str, cutoff: Any) -> dict:
    import datetime as dt

    min_date = pg_get_min_created_at(conn, table, order_by)
    if cutoff is None:
        cutoff = dt.datetime.now(dt.timezone.utc)

    archivable_rows = 0
    if min_date:
        archivable_rows = pg_count_rows(conn, table, min_date, cutoff, order_by)

    return {
        "min_date": min_date.strftime("%Y-%m") if min_date else None,
        "cutoff": cutoff.strftime("%Y-%m-%d"),
        "archivable_rows": archivable_rows,
    }


def _get_local_stats(base_dir: Path, table: str) -> dict:
    safe_table = table.replace(".", "_")
    parquet_dir = base_dir / "parquet" / safe_table

    stats: dict[str, Any] = {
        "chunk_count": 0,
        "total_size": 0,
        "total_rows": 0,
        "min_date": None,
        "max_date": None,
        "dates": [],
    }

    if not parquet_dir.exists():
        return stats

    for pf in parquet_dir.rglob("*.parquet"):
        if pf.name.endswith(".inprogress"):
            continue

        stats["chunk_count"] += 1
        stats["total_size"] += pf.stat().st_size

        manifest = load_manifest(pf.with_suffix(".parquet.manifest.json"))
        if manifest:
            stats["total_rows"] += manifest.get("exported_rows", 0)

        try:
            year_part = pf.parent.parent.name
            month_part = pf.parent.name
            if year_part.startswith("year=") and month_part.startswith("month="):
                year = int(year_part.split("=")[1])
                month = int(month_part.split("=")[1])
                stats["dates"].append((year, month))
        except (ValueError, IndexError):
            pass

    if stats["dates"]:
        stats["dates"].sort()
        stats["min_date"] = f"{stats['dates'][0][0]}-{stats['dates'][0][1]:02d}"
        stats["max_date"] = f"{stats['dates'][-1][0]}-{stats['dates'][-1][1]:02d}"

    return stats


def _get_s3_stats(s3: Any, bucket: str, prefix: str, table: str, mode: str) -> dict:
    safe_table = table.replace(".", "_")
    s3_prefix = f"{prefix}/backups/" if mode == "backup" else f"{prefix}/archive/{safe_table}/"

    stats = {"chunk_count": 0, "total_size": 0}

    try:
        paginator = s3.get_paginator("list_objects_v2")
        for page in paginator.paginate(Bucket=bucket, Prefix=s3_prefix):
            for obj in page.get("Contents", []):
                key = obj["Key"]
                if not key.endswith(".parquet"):
                    continue
                if mode == "backup" and safe_table not in key:
                    continue
                stats["chunk_count"] += 1
                stats["total_size"] += obj["Size"]
    except Exception as e:
        logger.debug(f"S3 error: {e}")

    return stats


def show_history(config: BackparqConfig, limit: int = 20, output_json: bool = False) -> list[dict]:
    """Show run history from catalog."""
    from backparq.adapters.catalog import Catalog

    # Use correct catalog path from pipeline.py logic: base_dir / "backparq.db"
    db_path = config.archive.base_dir / "backparq.db"

    if not db_path.exists():
        if not output_json:
            print_warning("No catalog found.")
        return []

    with Catalog(db_path) as catalog:
        history = catalog.get_history(limit)

    if output_json:
        print(json.dumps(history, indent=2, default=str))
        return history

    print_header("BACKPARQ HISTORY")
    table = Table(show_header=True, header_style="bold")
    table.add_column("Run ID")
    table.add_column("Mode")
    table.add_column("Started")
    table.add_column("Duration", justify="right")
    table.add_column("Status")
    table.add_column("Error")

    for run in history:
        import datetime as dt

        start = dt.datetime.fromisoformat(run["started_at"])
        duration = "-"
        if run["finished_at"]:
            end = dt.datetime.fromisoformat(run["finished_at"])
            delta = end - start
            duration = str(delta).split(".")[0]  # Remove microseconds

        status_style = {"completed": "green", "failed": "red", "running": "yellow"}.get(
            run["status"], "white"
        )

        table.add_row(
            run["id"],
            run["mode"],
            start.strftime("%Y-%m-%d %H:%M"),
            duration,
            f"[{status_style}]{run['status']}[/{status_style}]",
            run["error"] or "",
        )

    console.print(table)
    return history


def _get_catalog_stats(config: BackparqConfig) -> dict:
    from backparq.adapters.catalog import Catalog

    db_path = config.archive.base_dir / "backparq.db"
    if not db_path.exists():
        return {}

    with Catalog(db_path) as catalog:
        return catalog.get_stats()
