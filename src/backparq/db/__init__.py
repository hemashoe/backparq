"""Database connection and operations."""

from backparq.db.connection import ConnectionPool
from backparq.db.operations import (
    ChunkSpec,
    _parse_table_name,
    _table_identifier,
    add_months,
    connect_pg,
    delete_chunk_safely,
    delete_chunk_with_verification,
    export_chunk_to_parquet_streaming,
    insert_arrow_table_to_pg,
    list_chunks,
    month_floor,
    pg_count_rows,
    pg_get_columns,
    pg_get_min_created_at,
    table_exists,
    test_pg_connection,
    vacuum_table,
    validate_tables_exist,
)

__all__ = [
    "ChunkSpec",
    "connect_pg",
    "test_pg_connection",
    "table_exists",
    "validate_tables_exist",
    "pg_count_rows",
    "pg_get_min_created_at",
    "pg_get_columns",
    "list_chunks",
    "export_chunk_to_parquet_streaming",
    "delete_chunk_safely",
    "delete_chunk_with_verification",
    "insert_arrow_table_to_pg",
    "vacuum_table",
    "month_floor",
    "add_months",
    "ConnectionPool",
]
