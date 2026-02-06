from __future__ import annotations

import datetime as dt
import logging
from typing import Any

from backparq.archive import chunk_paths
from backparq.config import BackparqConfig
from backparq.db import connect_pg, list_chunks, pg_count_rows
from backparq.storage.parquet import load_manifest
from backparq.storage.s3 import create_client as s3_client_from_config

logger = logging.getLogger(__name__)


def plan_archive(config: BackparqConfig) -> dict[str, Any]:
    """
    Generate a plan of what chunks needs to be archived.

    Returns a dict structure suitable for JSON output.
    """
    plan: dict[str, Any] = {
        "tables": [],
        "summary": {
            "total_chunks_to_archive": 0,
            "total_rows_to_archive": 0,
            "total_chunks_existing": 0,
        },
    }

    s3 = None
    if config.s3.bucket:
        try:
            # Just instantiate, don't necessarily need to verify connection strict if we accept potential failure
            # But good to have s3 client for existence checks
            s3 = s3_client_from_config(config.s3)
        except Exception:
            pass

    conn = connect_pg(config.database)
    try:
        table_names = config.archive.table_names

        for table in table_names:
            table_plan: dict[str, Any] = {
                "name": table,
                "chunks": [],
                "stats": {"to_archive": 0, "existing": 0},
            }

            cutoff = config.archive.cutoff_exclusive
            if config.archive.mode == "backup" or cutoff is None:
                cutoff = dt.datetime.now(dt.timezone.utc) + dt.timedelta(days=1)

            chunks = list_chunks(conn, table, cutoff, config.archive.order_by)

            for chunk in chunks:
                chunk_info: dict[str, Any] = {
                    "start": chunk.start.isoformat(),
                    "end": chunk.end.isoformat(),
                    "rows_in_db": 0,
                    "action": "skip",
                    "reason": "unknown",
                }

                # Count rows
                db_rows = pg_count_rows(
                    conn, chunk.table, chunk.start, chunk.end, config.archive.order_by
                )
                chunk_info["rows_in_db"] = db_rows

                if db_rows == 0:
                    chunk_info["action"] = "skip"
                    chunk_info["reason"] = "empty"
                    table_plan["chunks"].append(chunk_info)
                    continue

                # Check if exists
                # This logic duplicates some of archive.py but simpler
                _, _, _, manifest_path = chunk_paths(config.archive.base_dir, chunk)
                existing = load_manifest(manifest_path)
                already_done = False

                if existing and not config.archive.overwrite:
                    expected = existing.get("exported_rows")
                    sha = existing.get("sha256", "")
                    # Ideally we check S3 too if configured
                    if config.s3.bucket and s3 and existing.get("s3_key"):
                        # Lightweight check? or full verify?
                        # For plan, maybe just assume manifest is correct or do quick head object?
                        # doing full verify might be slow for plan.
                        # check if we can trust manifest
                        already_done = True
                    elif expected is not None:
                        already_done = True  # Local only

                if already_done:
                    chunk_info["action"] = "skip"
                    chunk_info["reason"] = "already_archived"
                    table_plan["stats"]["existing"] += 1
                    plan["summary"]["total_chunks_existing"] += 1
                else:
                    chunk_info["action"] = "archive"
                    chunk_info["reason"] = "new_data" if not existing else "overwrite_needed"
                    table_plan["stats"]["to_archive"] += 1
                    plan["summary"]["total_chunks_to_archive"] += 1
                    plan["summary"]["total_rows_to_archive"] += db_rows

                table_plan["chunks"].append(chunk_info)

            plan["tables"].append(table_plan)

    finally:
        conn.close()

    return plan
