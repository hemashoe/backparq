"""Query interface using DuckDB."""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any, Optional

try:
    import duckdb
except ImportError:
    duckdb = None

from backparq.config import BackparqConfig
from backparq.utils.console import console, print_error

logger = logging.getLogger(__name__)


def _setup_duckdb(config: BackparqConfig) -> Optional[duckdb.DuckDBPyConnection]:
    """Initialize a DuckDB connection with S3 credentials configured."""
    if not duckdb:
        print_error("DuckDB not installed. Run 'pip install backparq[query]' to use this feature.")
        return None

    try:
        con = duckdb.connect(database=":memory:")

        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        if config.s3.bucket:
            if config.s3.region:
                con.execute(f"SET s3_region='{config.s3.region}';")

            if config.s3.access_key_id and config.s3.secret_access_key:
                con.execute(f"SET s3_access_key_id='{config.s3.access_key_id}';")
                con.execute(f"SET s3_secret_access_key='{config.s3.secret_access_key}';")

            if config.s3.session_token:
                con.execute(f"SET s3_session_token='{config.s3.session_token}';")

            if config.s3.endpoint_url:
                endpoint = config.s3.endpoint_url.replace("https://", "").replace("http://", "")
                con.execute(f"SET s3_endpoint='{endpoint}';")
                if not config.s3.use_ssl:
                    con.execute("SET s3_use_ssl=false;")

            if config.s3.addressing_style:
                con.execute(f"SET s3_url_style='{config.s3.addressing_style}';")

        return con
    except Exception as e:
        print_error(f"Failed to initialize DuckDB: {e}")
        return None


def run_query(config: BackparqConfig, sql_query: str) -> None:
    """Execute SQL query against S3 archives.

    Auto-registers tables in the config as DuckDB views.
    View name = table name with dots replaced by underscores.
    """
    con = _setup_duckdb(config)
    if not con:
        sys.exit(1)

    if config.s3.bucket:
        base_s3 = f"s3://{config.s3.bucket}/{config.s3.prefix}/archive"

        for table in config.archive.tables:
            view_name = table.name.replace(".", "_")
            s3_glob = f"{base_s3}/{view_name}/**/*.parquet"

            try:
                con.execute(
                    f"CREATE OR REPLACE VIEW {view_name} AS "
                    f"SELECT * FROM read_parquet('{s3_glob}');"
                )
                logger.debug(f"Registered view {view_name} -> {s3_glob}")
            except Exception as e:
                logger.warning(f"Could not register view for {table.name}: {e}")

    console.print(f"[bold]Executing:[/bold] {sql_query}")
    console.print()

    try:
        result = con.execute(sql_query).fetchall()
        columns = [desc[0] for desc in con.description]

        from rich.table import Table as RichTable

        output_table = RichTable(show_header=True, header_style="bold magenta")
        for col in columns:
            output_table.add_column(col)

        for row in result:
            output_table.add_row(*[str(x) for x in row])

        console.print(output_table)
        console.print(f"\n[dim]Returned {len(result)} rows[/dim]")

    except Exception as e:
        print_error(f"Query Execution Error: {e}")
        sys.exit(1)
