from __future__ import annotations

import datetime as dt
import logging
import re
import time
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import psycopg
from psycopg import sql
from psycopg.rows import dict_row

from backparq.config import BackparqConfig, DatabaseConfig
from backparq.db.connection import connect as connect_pg
from backparq.db.connection import test_connection as test_pg_connection
from backparq.primitives.chunking import add_months, month_floor, normalize_dt

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ChunkSpec:
    """Represents a time-bounded chunk of data to process."""

    table: str
    start: dt.datetime
    end: dt.datetime

    def __str__(self) -> str:
        return f"{self.table}[{self.start.strftime('%Y-%m')}]"


def _parse_table_name(table: str) -> tuple[Optional[str], str]:
    """Parse 'schema.table' into (schema, table) tuple."""
    if "." in table:
        parts = table.split(".", 1)
        return parts[0], parts[1]
    return None, table


def _table_identifier(table: str) -> sql.Composable:
    """Create a safe SQL identifier for a table name."""
    schema, table_name = _parse_table_name(table)
    if schema:
        return sql.Identifier(schema, table_name)
    return sql.Identifier(table_name)


def table_exists(conn: psycopg.Connection, table: str) -> bool:
    """Check if a table exists in the database."""
    schema, table_name = _parse_table_name(table)
    if schema:
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """)
        params: tuple[str, ...] = (schema, table_name)
    else:
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_name = %s
            )
        """)
        params = (table_name,)

    with conn.cursor() as cur:
        cur.execute(query, params)
        result = cur.fetchone()
        return bool(result[0]) if result else False


def validate_tables_exist(conn: psycopg.Connection, tables: list[str]) -> list[str]:
    """Validate that all tables exist. Returns list of missing tables."""
    missing = []
    for table in tables:
        if not table_exists(conn, table):
            missing.append(table)
    return missing


def pg_get_min_created_at(
    conn: psycopg.Connection, table: str, order_by: str = "created_at"
) -> Optional[dt.datetime]:
    """Get the minimum value of the order_by column in the table."""
    query = sql.SQL("SELECT min({column}) FROM {table}").format(
        column=sql.Identifier(order_by), table=_table_identifier(table)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        row = cur.fetchone()
        val = row[0] if row else None
        if val is None:
            return None
        return normalize_dt(val)


def pg_get_max_id(
    conn: psycopg.Connection,
    table: str,
    primary_key: str,
    cutoff: dt.datetime,
    order_by: str = "created_at",
) -> Any:
    """
    Get the maximum ID for rows created before the cutoff.
    This serves as the 'watermark' for safe deletion.
    """
    query = sql.SQL("SELECT max({pk}) FROM {table} WHERE {col} < %s").format(
        pk=sql.Identifier(primary_key), table=_table_identifier(table), col=sql.Identifier(order_by)
    )
    with conn.cursor() as cur:
        cur.execute(query, (cutoff,))
        row = cur.fetchone()
        val = row[0] if row else None
        return val


def pg_count_rows(
    conn: psycopg.Connection,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    order_by: str = "created_at",
) -> int:
    """Count rows in the given time range."""
    # This count is inherently approximate if not using snapshot isolation
    query = sql.SQL("SELECT count(*) FROM {table} WHERE {column} >= %s AND {column} < %s").format(
        table=_table_identifier(table), column=sql.Identifier(order_by)
    )
    with conn.cursor() as cur:
        cur.execute(query, (start, end))
        result = cur.fetchone()
        return int(result[0]) if result else 0


def pg_get_columns(conn: psycopg.Connection, table: str) -> list[str]:
    """Get list of column names for a table."""
    query = sql.SQL("SELECT * FROM {table} LIMIT 0").format(table=_table_identifier(table))
    with conn.cursor() as cur:
        cur.execute(query)
        if cur.description:
            return [desc.name for desc in cur.description]
        return []


def list_chunks(
    conn: psycopg.Connection,
    table: str,
    cutoff_exclusive: dt.datetime,
    order_by: str = "created_at",
    target_rows: Optional[int] = None,
) -> list[ChunkSpec]:
    """
    List chunks of data to process up to the cutoff date.

    If target_rows is provided, recursively splits time ranges until each chunk
    has <= target_rows (or duration is too small).
     otherwise, uses monthly chunks.
    """
    min_dt = pg_get_min_created_at(conn, table, order_by)
    if min_dt is None:
        logger.debug(f"Table {table} is empty, no chunks to process")
        return []

    start = month_floor(min_dt)
    cutoff = normalize_dt(cutoff_exclusive)
    if cutoff is None:
        cutoff = dt.datetime.now(dt.timezone.utc)

    chunks: list[ChunkSpec] = []

    # Adaptive chunking
    if target_rows:
        logger.info(f"Using adaptive chunking with target_rows={target_rows}")
        # Minimum duration to avoid infinite recursion (e.g. 1 hour)
        min_duration = dt.timedelta(hours=1)

        def _split_range(s: dt.datetime, e: dt.datetime) -> None:
            if e <= s:
                return

            # Count rows in this range
            count = pg_count_rows(conn, table, s, e, order_by)

            # If count is small enough OR range is too small, yield chunk
            if count <= target_rows or (e - s) <= min_duration:
                if count > 0:
                    chunks.append(ChunkSpec(table=table, start=s, end=e))
                return

            # Split in half
            mid_ts = s + (e - s) / 2
            _split_range(s, mid_ts)
            _split_range(mid_ts, e)

        cur = start
        while cur < cutoff:
            nxt = add_months(cur, 1)
            end = min(nxt, cutoff)
            _split_range(cur, end)
            cur = nxt

    else:
        # Standard monthly chunks
        cur = start
        while cur < cutoff:
            nxt = add_months(cur, 1)
            end = min(nxt, cutoff)
            chunks.append(ChunkSpec(table=table, start=cur, end=end))
            cur = nxt

    logger.debug(f"Found {len(chunks)} chunks for table {table}")
    return chunks


_SAFE_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _validate_identifier(name: str, label: str) -> str:
    """Validate that a name is a safe SQL identifier (no injection)."""
    if not _SAFE_IDENTIFIER.match(name):
        raise ValueError(
            f"Unsafe {label} identifier: {name!r}. "
            "Only alphanumeric characters and underscores are allowed."
        )
    return name


def export_chunk_to_parquet_streaming(
    conn: psycopg.Connection,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    parquet_path: Path,
    order_by: str,
    row_group_size: int,
    compression: str,
    encryption_properties: Any,
    masking: Optional[dict[str, str]] = None,
    watermark_id: Any = None,
    primary_key: str = "id",
    db_config: Optional[DatabaseConfig] = None,
    db_dsn: str = "",
) -> int:
    """Export chunk using DuckDB postgres_scanner. Returns row count.

    Args:
        db_config: DatabaseConfig object (preferred — builds DSN safely).
        db_dsn: Legacy fallback — raw DSN string. Use db_config instead.
    """
    import duckdb

    logger.info(
        f"Exporting {table} [{start.strftime('%Y-%m')}] via DuckDB"
        + (f" (watermark: {watermark_id})" if watermark_id else "")
    )

    if encryption_properties is not None:
        logger.warning("Parquet encryption is currently ignored with DuckDB export engine.")

    # Build DSN safely from config, falling back to raw string only if needed
    if db_config is not None:
        safe_dsn = db_config.duckdb_dsn().replace("'", "''")
    elif db_dsn:
        safe_dsn = db_dsn.replace("'", "''")
    else:
        raw_dsn = conn.info.dsn
        safe_dsn = raw_dsn.replace("'", "''")

    schema, table_name = _parse_table_name(table)
    schema = schema or "public"

    # Validate identifiers to prevent SQL injection via config values
    _validate_identifier(order_by, "order_by")
    _validate_identifier(primary_key, "primary_key")
    _validate_identifier(schema, "schema")
    _validate_identifier(table_name, "table_name")

    columns = pg_get_columns(conn, table)
    select_exprs = []
    masking = masking or {}
    for col in columns:
        _validate_identifier(col, "column")
        if col in masking:
            rule = masking[col]
            if rule == "hash":
                select_exprs.append(f'sha256(CAST("{col}" AS VARCHAR)) AS "{col}"')
            elif rule == "redact":
                select_exprs.append(f"'***REDACTED***' AS \"{col}\"")
            elif rule == "partial":
                select_exprs.append(
                    f'CASE WHEN length(CAST("{col}" AS VARCHAR)) > 4 '
                    f'THEN repeat(\'*\', CAST(length(CAST("{col}" AS VARCHAR)) AS INTEGER) - 4) || right(CAST("{col}" AS VARCHAR), 4) '
                    f'ELSE CAST("{col}" AS VARCHAR) END AS "{col}"'
                )
            else:
                select_exprs.append(f'"{col}"')
        else:
            select_exprs.append(f'"{col}"')

    select_clause = ", ".join(select_exprs)

    where_clauses = [
        f"\"{order_by}\" >= '{start.isoformat()}'::TIMESTAMP",
        f"\"{order_by}\" < '{end.isoformat()}'::TIMESTAMP",
    ]
    if watermark_id is not None:
        if isinstance(watermark_id, str):
            # String watermarks need quoting
            safe_wm = str(watermark_id).replace("'", "''")
            where_clauses.append(f"\"{primary_key}\" <= '{safe_wm}'")
        else:
            where_clauses.append(f'"{primary_key}" <= {watermark_id}')

    where_clause = " AND ".join(where_clauses)

    # DuckDB format parameters
    codec = compression.upper() if compression else "SNAPPY"
    if codec == "NONE":
        codec = "UNCOMPRESSED"

    query = f"""
    COPY (
        SELECT {select_clause}
        FROM postgres_scan('{safe_dsn}', '{schema}', '{table_name}')
        WHERE {where_clause}
        ORDER BY "{order_by}"
    ) TO '{parquet_path.as_posix()}' (FORMAT PARQUET, COMPRESSION '{codec}', ROW_GROUP_SIZE {row_group_size});
    """

    try:
        con = duckdb.connect()
        con.execute("INSTALL postgres;")
        con.execute("LOAD postgres;")

        con.execute(query)

        # COPY TO does not reliably return a row count across DuckDB versions.
        # Read the row count from the written file instead.
        count_row = con.execute(
            f"SELECT COUNT(*) FROM read_parquet('{parquet_path.as_posix()}')"
        ).fetchone()
        exported = count_row[0] if count_row else 0

        logger.info(f"Export complete: {exported} rows written to {parquet_path}")
        return exported
    except Exception as e:
        logger.error(f"DuckDB Export failed for {table}: {e}")
        raise


def delete_chunk_safely(
    conn: psycopg.Connection,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    batch_size: int,
    order_by: str = "created_at",
    watermark_id: Any = None,
    primary_key: str = "id",
) -> int:
    """Delete rows in batches. Returns total deleted."""
    logger.info(f"Deleting {table} [{start.strftime('%Y-%m')}]")
    total = 0

    # If watermark is missing, we fall back to time-only deletion (DANGEROUS but backwards compatible)
    if watermark_id is not None:
        where_clause = sql.SQL("{column} >= %s AND {column} < %s AND {pk} <= %s").format(
            column=sql.Identifier(order_by), pk=sql.Identifier(primary_key)
        )
        params: tuple = (start, end, watermark_id, batch_size)
    else:
        logger.warning(f"Deleting {table} without watermark! Race conditions possible.")
        where_clause = sql.SQL("{column} >= %s AND {column} < %s").format(
            column=sql.Identifier(order_by)
        )
        params = (start, end, batch_size)

    # Build the delete query with safe identifiers
    delete_query = sql.SQL("""
        WITH cte AS (
            SELECT ctid
            FROM {table}
            WHERE {where}
            LIMIT %s
        )
        DELETE FROM {table} t
        USING cte
        WHERE t.ctid = cte.ctid
        RETURNING 1
    """).format(table=_table_identifier(table), where=where_clause)

    with conn.cursor() as cur:
        batch_num = 0
        while True:
            cur.execute(delete_query, params)
            deleted = cur.rowcount
            conn.commit()
            total += deleted
            batch_num += 1

            if deleted > 0:
                logger.debug(f"Deleted batch {batch_num}: {deleted} rows (total: {total})")

            if deleted == 0:
                break

    logger.info(f"Deletion complete: {total} rows removed from {table}")
    return total


def pg_get_partitions_in_range(
    conn: psycopg.Connection,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    order_by: str,
) -> list[str]:
    """Find partitions of a table that contain data in the given time range.

    Uses pg_inherits + pg_class for reliable partition discovery instead of
    EXPLAIN heuristics which can miss partitions based on planner statistics.
    """
    schema, table_name = _parse_table_name(table)
    parent_schema = schema or "public"

    # Get all child partitions from pg_inherits
    query = sql.SQL("""
        SELECT
            child_ns.nspname || '.' || child_class.relname AS partition_name
        FROM pg_inherits
        JOIN pg_class parent_class ON pg_inherits.inhparent = parent_class.oid
        JOIN pg_namespace parent_ns ON parent_class.relnamespace = parent_ns.oid
        JOIN pg_class child_class ON pg_inherits.inhrelid = child_class.oid
        JOIN pg_namespace child_ns ON child_class.relnamespace = child_ns.oid
        WHERE parent_ns.nspname = %s
          AND parent_class.relname = %s
    """)

    with conn.cursor() as cur:
        cur.execute(query, (parent_schema, table_name))
        all_partitions = [row[0] for row in cur.fetchall()]

    if not all_partitions:
        return []

    # Filter partitions: check which ones actually contain data in the time range
    partitions_with_data = []
    for partition in all_partitions:
        count_query = sql.SQL(
            "SELECT EXISTS(SELECT 1 FROM {table} WHERE {col} >= %s AND {col} < %s LIMIT 1)"
        ).format(
            table=_table_identifier(partition),
            col=sql.Identifier(order_by),
        )
        with conn.cursor() as cur:
            cur.execute(count_query, (start, end))
            row = cur.fetchone()
            if row and row[0]:
                partitions_with_data.append(partition)

    return partitions_with_data


def detach_and_drop_partition(conn: psycopg.Connection, parent: str, partition: str) -> None:
    """Detach a partition and drop it natively."""
    logger.info(f"Detaching partition {partition} from {parent}")
    with conn.cursor() as cur:
        cur.execute(
            sql.SQL("ALTER TABLE {parent} DETACH PARTITION {child}").format(
                parent=_table_identifier(parent), child=_table_identifier(partition)
            )
        )
        cur.execute(sql.SQL("DROP TABLE {child}").format(child=_table_identifier(partition)))
    conn.commit()
    logger.info(f"Partition {partition} successfully detached and dropped.")


def delete_chunk_with_verification(
    conn: psycopg.Connection,
    table: str,
    expected_sha256: str,
    s3_bucket: str,
    s3_key: str,
    s3_client: Any,
    start: dt.datetime,
    end: dt.datetime,
    order_by: str,
    config: BackparqConfig,
    watermark_id: Any = None,
) -> int:
    """Delete chunk after verifying S3 backup exists and matches checksum.

    Returns:
        Number of rows deleted. Returns -1 if S3 verification failed.
    """
    from backparq.storage.s3 import verify_checksum as s3_verify_object_sha256

    logger.info(f"Pre-delete verification for {table} [{start.strftime('%Y-%m')}]")

    if not s3_verify_object_sha256(s3_client, s3_bucket, s3_key, expected_sha256):
        logger.error(f"S3 verification failed for {s3_key}. Data NOT deleted.")
        return -1

    # Get Primary Key from config
    table_config = config.archive.get_table_config(table)
    primary_key = table_config.primary_key

    # Proceed based on offload strategy
    offload_strategy = getattr(config.archive, "offload_strategy", "delete")

    if offload_strategy == "detach":
        partitions = pg_get_partitions_in_range(conn, table, start, end, order_by)
        if not partitions:
            logger.info(f"No partitions found for {table} in time range, skipping detach.")
            return 0
        else:
            for part in partitions:
                detach_and_drop_partition(conn, table, part)
            return 0  # Rows are dropped with the partition, count unknown
    else:
        batch_size = getattr(config.archive, "delete_batch_size", 10000)

        rows_deleted = delete_chunk_safely(
            conn=conn,
            table=table,
            start=start,
            end=end,
            batch_size=batch_size,
            order_by=order_by,
            watermark_id=watermark_id,
            primary_key=primary_key,
        )

        return rows_deleted


def _serialize_for_postgres(value: Any) -> Any:
    """Serialize Python values for PostgreSQL COPY format."""
    import json

    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, list):
        parts = []
        for x in value:
            if x is None:
                parts.append("NULL")
            elif isinstance(x, str):
                escaped = x.replace("\\", "\\\\").replace('"', '\\"')
                parts.append(f'"{escaped}"')
            else:
                parts.append(str(x))
        return "{" + ",".join(parts) + "}"
    return value


def insert_arrow_table_to_pg(
    conn: psycopg.Connection,
    table: str,
    arrow_table: Any,
    conflict_mode: str = "do_nothing",
    primary_key: str = "id",
    batch_size: int = 10_000,
) -> int:
    """
    Insert Arrow table data into PostgreSQL using COPY for efficiency.
    """
    import csv
    import io

    if arrow_table.num_rows == 0:
        return 0

    logger.info(f"Inserting {arrow_table.num_rows} rows into {table} (mode: {conflict_mode})")

    # Check for complex types that pyarrow.csv might not handle compatibly with Postgres
    import pyarrow as pa

    has_complex_types = False
    for field in arrow_table.schema:
        if pa.types.is_nested(field.type) or pa.types.is_dictionary(field.type):
            has_complex_types = True
            break

    total_inserted = 0
    columns = arrow_table.column_names
    cols_quoted = [sql.Identifier(c) for c in columns]
    cols_str = sql.SQL(",").join(cols_quoted)

    for batch in arrow_table.to_batches(max_chunksize=batch_size):
        if batch.num_rows == 0:
            continue

        buf = io.BytesIO()

        if not has_complex_types:
            # FAST PATH: Use pyarrow.csv
            import pyarrow.csv as pacsv

            write_options = pacsv.WriteOptions(
                include_header=False, delimiter="\t", quoting_style="needed"
            )
            pacsv.write_csv(batch, buf, write_options=write_options)
        else:
            # SLOW PATH: Manual serialization
            text_buf = io.StringIO()
            writer = csv.writer(text_buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar='"')

            rows = batch.to_pylist()
            for row in rows:
                csv_row = [_serialize_for_postgres(row[c]) for c in columns]
                writer.writerow(csv_row)

            text_buf.seek(0)
            buf.write(text_buf.getvalue().encode("utf-8"))

        buf.seek(0)

        stage_table = f"stage_{uuid.uuid4().hex[:12]}"
        stage_ident = sql.Identifier(stage_table)

        with conn.cursor() as cur:
            # Create temp staging table
            create_stage = sql.SQL(
                "CREATE TEMP TABLE IF NOT EXISTS {stage} (LIKE {table} INCLUDING ALL) ON COMMIT DELETE ROWS"
            ).format(stage=stage_ident, table=_table_identifier(table))
            cur.execute(create_stage)

            # COPY data into staging table
            copy_sql = sql.SQL(
                "COPY {stage} ({cols}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', QUOTE '\"', NULL '')"
            ).format(stage=stage_ident, cols=cols_str)

            # psycopg3 copy interface
            with cur.copy(copy_sql) as copy:
                copy.write(buf.getvalue())

            # Build INSERT/UPSERT query
            pk_ident = sql.Identifier(primary_key)
            on_conflict: sql.Composable
            if conflict_mode == "upsert":
                set_clause = sql.SQL(", ").join(
                    sql.SQL("{col} = EXCLUDED.{col}").format(col=sql.Identifier(c))
                    for c in columns
                    if c != primary_key
                )
                on_conflict = sql.SQL("ON CONFLICT ({pk}) DO UPDATE SET {sets}").format(
                    pk=pk_ident, sets=set_clause
                )
            else:
                on_conflict = sql.SQL("ON CONFLICT DO NOTHING")

            insert_sql = sql.SQL("""
                INSERT INTO {table} ({cols})
                SELECT {cols} FROM {stage}
                {on_conflict}
            """).format(
                table=_table_identifier(table),
                cols=cols_str,
                stage=stage_ident,
                on_conflict=on_conflict,
            )
            cur.execute(insert_sql)
            total_inserted += cur.rowcount

            # Clear staging table
            truncate_sql = sql.SQL("TRUNCATE {stage}").format(stage=stage_ident)
            cur.execute(truncate_sql)

    conn.commit()
    logger.info(f"Insert complete: {total_inserted} rows affected")
    return total_inserted


def vacuum_table(conn: psycopg.Connection, table: str) -> None:
    """
    Run VACUUM ANALYZE on a table to reclaim storage.

    Must run outside of a transaction block (autocommit=True).
    """
    logger.info(f"Vacuuming table {table}...")

    # Assumes conn is already autocommit if needed, or we must ensure it.
    # Caller should handle connection state.
    try:
        with conn.cursor() as cur:
            query = sql.SQL("VACUUM ANALYZE {table}").format(table=_table_identifier(table))
            cur.execute(query)
        logger.info(f"Vacuum complete for {table}")
    except Exception as e:
        logger.error(f"Vacuum failed for {table}: {e}")
