"""
Backparq Database Module

Handles PostgreSQL connections, data export, and deletion operations.
Uses parameterized queries to prevent SQL injection.
"""

import datetime as dt
import logging
import time
from dataclasses import dataclass
from typing import Optional

import psycopg2
import psycopg2.extensions
import psycopg2.extras
from psycopg2 import sql
from tenacity import retry, stop_after_attempt, wait_exponential

from backparq.config import DatabaseConfig

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


@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def connect_pg(config: DatabaseConfig):
    """
    Create a new PostgreSQL connection with retry logic.

    Note: In production, consider using a connection pool (e.g., psycopg2.pool)
    for better resource management with concurrent operations.
    """
    logger.debug(f"Connecting to PostgreSQL at {config.host}:{config.port}/{config.name}")
    conn = psycopg2.connect(config.dsn())
    conn.autocommit = False
    logger.debug("PostgreSQL connection established")
    return conn


def test_pg_connection(config: DatabaseConfig) -> None:
    """Test that we can connect to PostgreSQL."""
    logger.info(f"Testing PostgreSQL connection to {config.host}:{config.port}/{config.name}")
    conn = connect_pg(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
        logger.info("PostgreSQL connection test successful")
    finally:
        conn.close()


def table_exists(conn, table: str) -> bool:
    """Check if a table exists in the database."""
    schema, table_name = _parse_table_name(table)
    if schema:
        query = sql.SQL("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            )
        """)
        params = (schema, table_name)
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
        return cur.fetchone()[0]


def validate_tables_exist(conn, tables: list[str]) -> list[str]:
    """Validate that all tables exist. Returns list of missing tables."""
    missing = []
    for table in tables:
        if not table_exists(conn, table):
            missing.append(table)
    return missing


def _normalize_dt(value: dt.datetime) -> dt.datetime:
    """Normalize a datetime to UTC timezone-aware."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def month_floor(value: dt.datetime) -> dt.datetime:
    """Return the first day of the month for the given datetime."""
    value = _normalize_dt(value)
    return dt.datetime(value.year, value.month, 1, tzinfo=dt.timezone.utc)


def add_months(value: dt.datetime, months: int) -> dt.datetime:
    """Add months to a datetime, returning the first of the resulting month."""
    value = _normalize_dt(value)
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    return dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)


def pg_get_min_created_at(conn, table: str, order_by: str = "created_at") -> Optional[dt.datetime]:
    """Get the minimum value of the order_by column in the table."""
    query = sql.SQL("SELECT min({column}) FROM {table}").format(
        column=sql.Identifier(order_by), table=_table_identifier(table)
    )
    with conn.cursor() as cur:
        cur.execute(query)
        val = cur.fetchone()[0]
        if val is None:
            return None
        return _normalize_dt(val)


def pg_count_rows(
    conn, table: str, start: dt.datetime, end: dt.datetime, order_by: str = "created_at"
) -> int:
    """Count rows in the given time range."""
    query = sql.SQL("SELECT count(*) FROM {table} WHERE {column} >= %s AND {column} < %s").format(
        table=_table_identifier(table), column=sql.Identifier(order_by)
    )
    with conn.cursor() as cur:
        cur.execute(query, (start, end))
        return int(cur.fetchone()[0])


def pg_get_columns(conn, table: str) -> list[str]:
    """Get list of column names for a table."""
    query = sql.SQL("SELECT * FROM {table} LIMIT 0").format(table=_table_identifier(table))
    with conn.cursor() as cur:
        cur.execute(query)
        return [desc[0] for desc in cur.description]


def list_chunks(
    conn, table: str, cutoff_exclusive: dt.datetime, order_by: str = "created_at"
) -> list[ChunkSpec]:
    """
    List monthly chunks of data to process up to the cutoff date.

    Returns a list of ChunkSpec objects, each representing one month of data.
    """
    min_dt = pg_get_min_created_at(conn, table, order_by)
    if min_dt is None:
        logger.debug(f"Table {table} is empty, no chunks to process")
        return []

    start = month_floor(min_dt)
    cutoff = _normalize_dt(cutoff_exclusive)

    chunks: list[ChunkSpec] = []
    cur = start
    while cur < cutoff:
        nxt = add_months(cur, 1)
        end = min(nxt, cutoff)
        chunks.append(ChunkSpec(table=table, start=cur, end=end))
        cur = nxt

    logger.debug(f"Found {len(chunks)} chunks for table {table} from {start} to {cutoff}")
    return chunks


def export_chunk_to_parquet_streaming(
    conn,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    parquet_path,
    order_by: str,
    fetch_size: int,
    compression: str,
    encryption_properties,
) -> int:
    """
    Stream rows from PostgreSQL to a Parquet file using server-side cursors.

    Uses REPEATABLE READ isolation to ensure consistent snapshot of data.
    Memory usage is constant regardless of table size due to streaming.

    Returns the number of rows exported.
    """
    logger.info(f"Exporting {table} [{start.strftime('%Y-%m')}] to {parquet_path}")
    exported = 0
    writer = None
    schema = None

    try:
        conn.rollback()
        conn.set_session(
            readonly=True,
            isolation_level=psycopg2.extensions.ISOLATION_LEVEL_REPEATABLE_READ,
            autocommit=False,
        )

        cursor_name = f"csr_{int(time.time() * 1000)}"

        # Build query using safe identifiers
        query = sql.SQL(
            "SELECT * FROM {table} WHERE {column} >= %s AND {column} < %s ORDER BY {column}"
        ).format(table=_table_identifier(table), column=sql.Identifier(order_by))

        with conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.itersize = fetch_size
            cur.execute(query, (start, end))

            batch_count = 0
            while True:
                rows = cur.fetchmany(fetch_size)
                if not rows:
                    break

                if schema is None:
                    import pyarrow as pa
                    import pyarrow.parquet as pq

                    t0 = pa.Table.from_pylist(rows)
                    schema = pa.schema([pa.field(f.name, f.type, nullable=True) for f in t0.schema])
                    writer = pq.ParquetWriter(
                        parquet_path.as_posix(),
                        schema,
                        compression=compression,
                        encryption_properties=encryption_properties,
                    )
                    logger.debug(f"Initialized Parquet writer with {len(schema)} columns")

                assert schema is not None and writer is not None
                rb = pa.Table.from_pylist(rows, schema=schema).to_batches(max_chunksize=len(rows))[
                    0
                ]
                writer.write_batch(rb)
                exported += len(rows)
                batch_count += 1

                if batch_count % 10 == 0:
                    logger.debug(f"Exported {exported} rows so far...")

        if writer is not None:
            writer.close()

        conn.commit()
        logger.info(f"Export complete: {exported} rows written to {parquet_path}")
        return exported

    except Exception as e:
        logger.error(f"Export failed for {table}: {e}")
        raise
    finally:
        try:
            conn.rollback()
        except Exception:
            pass
        conn.set_session(
            readonly=False,
            isolation_level=psycopg2.extensions.ISOLATION_LEVEL_READ_COMMITTED,
            autocommit=False,
        )


def delete_chunk_safely(
    conn,
    table: str,
    start: dt.datetime,
    end: dt.datetime,
    batch_size: int,
    order_by: str = "created_at",
) -> int:
    """
    Delete rows in batches using ctid to minimize lock contention.

    Uses CTE with LIMIT to process small batches and commits after each batch.
    This prevents long-running transactions that could block other operations.

    Returns the total number of rows deleted.
    """
    logger.info(
        f"Deleting data from {table} [{start.strftime('%Y-%m')} to {end.strftime('%Y-%m')}]"
    )
    total = 0

    # Build the delete query with safe identifiers
    delete_query = sql.SQL("""
        WITH cte AS (
            SELECT ctid
            FROM {table}
            WHERE {column} >= %s AND {column} < %s
            LIMIT %s
        )
        DELETE FROM {table} t
        USING cte
        WHERE t.ctid = cte.ctid
        RETURNING 1
    """).format(table=_table_identifier(table), column=sql.Identifier(order_by))

    with conn.cursor() as cur:
        batch_num = 0
        while True:
            cur.execute(delete_query, (start, end, batch_size))
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


def _serialize_for_postgres(value):
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
    conn,
    table: str,
    arrow_table,
    conflict_mode: str = "do_nothing",
    primary_key: str = "id",
    batch_size: int = 10_000,
) -> int:
    """
    Insert Arrow table data into PostgreSQL using COPY for efficiency.

    Supports two conflict modes:
    - "do_nothing": Skip rows with conflicting primary keys
    - "upsert": Update existing rows with new values

    Uses a staging table pattern for atomic upserts.

    Returns the number of rows inserted/updated.
    """
    import csv
    import io

    if arrow_table.num_rows == 0:
        return 0

    logger.info(f"Inserting {arrow_table.num_rows} rows into {table} (mode: {conflict_mode})")
    total_inserted = 0
    columns = arrow_table.column_names
    cols_quoted = [sql.Identifier(c) for c in columns]
    cols_str = sql.SQL(",").join(cols_quoted)

    for batch in arrow_table.to_batches(max_chunksize=batch_size):
        rows = batch.to_pylist()
        if not rows:
            continue

        buf = io.StringIO()
        writer = csv.writer(buf, delimiter="\t", quoting=csv.QUOTE_MINIMAL, quotechar='"')

        for row in rows:
            csv_row = [_serialize_for_postgres(row[c]) for c in columns]
            writer.writerow(csv_row)

        buf.seek(0)

        stage_table = f"stage_{int(time.time() * 1000000)}"
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
            cur.copy_expert(copy_sql.as_string(conn), buf)

            # Build INSERT/UPSERT query
            pk_ident = sql.Identifier(primary_key)
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
