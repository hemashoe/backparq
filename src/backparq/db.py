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
