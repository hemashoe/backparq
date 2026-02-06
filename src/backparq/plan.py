"""Archive planning — generate a plan of chunks to archive."""

from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path
from typing import Any

from backparq.config import BackparqConfig
from backparq.db import connect_pg, list_chunks, pg_count_rows
from backparq.primitives import chunk_paths
from backparq.storage.parquet import load_manifest
from backparq.storage.s3 import create_client as s3_client_from_config

logger = logging.getLogger(__name__)


def plan_archive(config: BackparqConfig) -> dict[str, Any]:
    """Generate a plan of chunks that need to be archived.

    Checks both the local catalog (if available) and S3 manifests
    to determine which chunks still need processing.

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

    # Try loading catalog for state awareness
    catalog = None
    catalog_path = config.archive.base_dir / "backparq.db"
    if catalog_path.exists():
        from backparq.adapters.catalog import Catalog, ChunkState

        catalog = Catalog(catalog_path)

    s3 = None
    if config.s3.bucket:
        try:
            s3 = s3_client_from_config(config.s3)
        except Exception:
            pass

    conn = connect_pg(config.database)
    try:
        for table in config.archive.table_names:
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

                db_rows = pg_count_rows(
                    conn, chunk.table, chunk.start, chunk.end, config.archive.order_by
                )
                chunk_info["rows_in_db"] = db_rows

                if db_rows == 0:
                    chunk_info["action"] = "skip"
                    chunk_info["reason"] = "empty"
                    table_plan["chunks"].append(chunk_info)
                    continue

                # Check catalog first (authoritative state)
                already_done = False
                chunk_id = f"{chunk.table}_{chunk.start.strftime('%Y%m%d%H%M%S')}"

                if catalog:
                    state = catalog.get_state(chunk_id)
                    if state and state >= ChunkState.EXPORTED:
                        already_done = True

                # Fallback: check manifest files
                if not already_done:
                    _, _, _, manifest_path = chunk_paths(config.archive.base_dir, chunk)
                    existing = load_manifest(manifest_path)

                    if existing and not config.archive.overwrite:
                        if s3 and config.s3.bucket and existing.get("s3_key"):
                            already_done = True
                        elif existing.get("exported_rows") is not None:
                            already_done = True

                if already_done:
                    chunk_info["action"] = "skip"
                    chunk_info["reason"] = "already_archived"
                    table_plan["stats"]["existing"] += 1
                    plan["summary"]["total_chunks_existing"] += 1
                else:
                    chunk_info["action"] = "archive"
                    chunk_info["reason"] = "new_data" if not catalog else "pending"
                    table_plan["stats"]["to_archive"] += 1
                    plan["summary"]["total_chunks_to_archive"] += 1
                    plan["summary"]["total_rows_to_archive"] += db_rows

                table_plan["chunks"].append(chunk_info)

            plan["tables"].append(table_plan)

    finally:
        conn.close()

    return plan
