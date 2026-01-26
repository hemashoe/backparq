import datetime as dt
import time
from dataclasses import dataclass
from typing import Optional

import psycopg2
import psycopg2.extras
import psycopg2.extensions

from backparq.config import DatabaseConfig


@dataclass(frozen=True)
class ChunkSpec:
    table: str
    start: dt.datetime
    end: dt.datetime


from tenacity import retry, stop_after_attempt, wait_exponential

@retry(stop=stop_after_attempt(5), wait=wait_exponential(multiplier=1, min=2, max=30))
def connect_pg(config: DatabaseConfig):
    conn = psycopg2.connect(config.dsn())
    conn.autocommit = False
    return conn


def test_pg_connection(config: DatabaseConfig) -> None:
    conn = connect_pg(config)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
            cur.fetchone()
    finally:
        conn.close()


def _normalize_dt(value: dt.datetime) -> dt.datetime:
    if value.tzinfo is None:
        value = value.replace(tzinfo=dt.timezone.utc)
    return value.astimezone(dt.timezone.utc)


def month_floor(value: dt.datetime) -> dt.datetime:
    value = _normalize_dt(value)
    return dt.datetime(value.year, value.month, 1, tzinfo=dt.timezone.utc)


def add_months(value: dt.datetime, months: int) -> dt.datetime:
    value = _normalize_dt(value)
    year = value.year + (value.month - 1 + months) // 12
    month = (value.month - 1 + months) % 12 + 1
    return dt.datetime(year, month, 1, tzinfo=dt.timezone.utc)


def pg_get_min_created_at(conn, table: str) -> Optional[dt.datetime]:
    with conn.cursor() as cur:
        cur.execute(f"SELECT min(created_at) FROM {table}")
        val = cur.fetchone()[0]
        if val is None:
            return None
        return _normalize_dt(val)


def pg_count_rows(conn, table: str, start: dt.datetime, end: dt.datetime) -> int:
    with conn.cursor() as cur:
        cur.execute(
            f"SELECT count(*) FROM {table} WHERE created_at >= %s AND created_at < %s",
            (start, end),
        )
        return int(cur.fetchone()[0])


def pg_get_columns(conn, table: str) -> list[str]:
    """Returns a list of column names for the given table."""
    # We can use information_schema or just select * limit 0
    # efficient way:
    with conn.cursor() as cur:
        cur.execute(f"SELECT * FROM {table} LIMIT 0")
        return [desc[0] for desc in cur.description]


def list_chunks(conn, table: str, cutoff_exclusive: dt.datetime) -> list[ChunkSpec]:
    min_dt = pg_get_min_created_at(conn, table)
    if min_dt is None:
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
    Streams rows from Postgres to Parquet using a server-side cursor.

    IMPORTANT: Transaction snapshot is set on the connection (not executed via SQL on named cursor).
    """
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
        with conn.cursor(name=cursor_name, cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            q = (
                f"SELECT * FROM {table} "
                f"WHERE created_at >= %s AND created_at < %s "
                f"ORDER BY {order_by}"
            )
            cur.itersize = fetch_size
            cur.execute(q, (start, end))

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
                assert schema is not None and writer is not None
                rb = (
                    pa.Table.from_pylist(rows, schema=schema)
                    .to_batches(max_chunksize=len(rows))[0]
                )
                writer.write_batch(rb)
                exported += len(rows)

        if writer is not None:
            writer.close()

        conn.commit()
        return exported

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
) -> int:
    """
    Deletes in batches using ctid to avoid long locks.
    """
    total = 0
    with conn.cursor() as cur:
        while True:
            cur.execute(
                f"""
                WITH cte AS (
                    SELECT ctid
                    FROM {table}
                    WHERE created_at >= %s AND created_at < %s
                    LIMIT %s
                )
                DELETE FROM {table} t
                USING cte
                WHERE t.ctid = cte.ctid
                RETURNING 1
                """,
                (start, end, batch_size),
            )
            deleted = cur.rowcount
            conn.commit()
            total += deleted
            if deleted == 0:
                break
    return total


def _serialize_for_postgres(value):
    import json
    if isinstance(value, dict):
        return json.dumps(value)
    if isinstance(value, list):
        parts = []
        for x in value:
            if x is None:
                parts.append("NULL")
            elif isinstance(x, str):
                parts.append(f'"{x.replace(chr(92), chr(92)*2).replace(chr(34), chr(92)+chr(34))}"')
            else:
                parts.append(str(x))
        return "{" + ",".join(parts) + "}"
    return value


def insert_arrow_table_to_pg(
    conn,
    table: str,
    arrow_table,
    conflict_mode: str = "do_nothing",
    primary_key: Optional[str] = "id",
    batch_size: int = 10_000,
) -> int:
    import io
    import csv

    if arrow_table.num_rows == 0:
        return 0
    
    total_inserted = 0

    for batch in arrow_table.to_batches(max_chunksize=batch_size):
        rows = batch.to_pylist()
        if not rows:
            continue
            
        buf = io.StringIO()
        writer = csv.writer(buf, delimiter='\t', quoting=csv.QUOTE_MINIMAL, quotechar='"')
        
        columns = arrow_table.column_names
        
        for row in rows:
            csv_row = [_serialize_for_postgres(row[c]) for c in columns]
            writer.writerow(csv_row)
        
        buf.seek(0)
        
        cols_str = ",".join(f'"{c}"' for c in columns)
        
        stage_table = f"stage_{int(time.time() * 1000000)}"

        with conn.cursor() as cur:
             # Create temp table if not exists (or always create/drop)
            cur.execute(f"CREATE TEMP TABLE IF NOT EXISTS {stage_table} (LIKE {table} INCLUDING ALL) ON COMMIT DELETE ROWS")
            
            # COPY
            # Use CSV format (Tab delimited to avoid comma issues, passed to csv.writer)
            cur.copy_expert(f"COPY {stage_table} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\t', QUOTE '\"', NULL '')", buf)
            
            # INSERT / UPSERT
            if conflict_mode == "upsert":
                if not primary_key:
                    raise ValueError("primary_key is required for upsert mode")
                    
                set_clause = ", ".join(
                    f'"{c}" = EXCLUDED."{c}"' for c in columns if c != primary_key
                )
                on_conflict = f"ON CONFLICT ({primary_key}) DO UPDATE SET {set_clause}"
            else:
                on_conflict = "ON CONFLICT DO NOTHING"

            insert_sql = f"""
                INSERT INTO {table} ({cols_str})
                SELECT {cols_str} FROM {stage_table}
                {on_conflict}
            """
            cur.execute(insert_sql)
            total_inserted += cur.rowcount
            
            # Clear staging
            cur.execute(f"TRUNCATE {stage_table}")
    
    conn.commit()
    return total_inserted
