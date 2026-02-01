"""Database connection and operations."""

from backparq.db.operations import (
    ChunkSpec,
    connect_pg,
    test_pg_connection,
    table_exists,
    validate_tables_exist,
    pg_count_rows,
    pg_get_min_created_at,
    pg_get_columns,
    list_chunks,
    export_chunk_to_parquet_streaming,
    delete_chunk_safely,
    delete_chunk_with_verification,
    insert_arrow_table_to_pg,
    vacuum_table,
    month_floor,
    add_months,
    _table_identifier,
    _parse_table_name,
)

from backparq.db.connection import ConnectionPool

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
