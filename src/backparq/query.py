"""Query interface using DuckDB."""

from __future__ import annotations

import logging
import sys
from typing import Optional

try:
    import duckdb
except ImportError:
    duckdb = None

from backparq.config import BackparqConfig
from backparq.utils.console import console, print_error

logger = logging.getLogger(__name__)


def _setup_duckdb(config: BackparqConfig) -> Optional[duckdb.DuckDBPyConnection]:
    if not duckdb:
        print_error("DuckDB not installed. Run 'pip install backparq[query]' to use this feature.")
        return None

    try:
        con = duckdb.connect(database=":memory:")

        # Install/Load httpfs for S3 support
        con.execute("INSTALL httpfs;")
        con.execute("LOAD httpfs;")

        # Configure S3 credentials
        if config.s3.bucket:
            # Use region if available
            region_config = f"SET s3_region='{config.s3.region}';" if config.s3.region else ""

            # Credentials (prefer env vars, but explicit set for DuckDB session)
            creds_config = ""
            if config.s3.access_key_id and config.s3.secret_access_key:
                creds_config = f"""
                    SET s3_access_key_id='{config.s3.access_key_id}';
                    SET s3_secret_access_key='{config.s3.secret_access_key}';
                """
            if config.s3.session_token:
                creds_config += f"SET s3_session_token='{config.s3.session_token}';"

            # Endpoint for MinIO/custom S3
            endpoint_config = ""
            if config.s3.endpoint_url:
                endpoint = config.s3.endpoint_url.replace("https://", "").replace("http://", "")
                endpoint_config = f"SET s3_endpoint='{endpoint}';"
                if not config.s3.use_ssl:
                    endpoint_config += "SET s3_use_ssl=false;"

            style_config = ""
            if config.s3.addressing_style:
                style_config = f"SET s3_url_style='{config.s3.addressing_style}';"

            setup_sql = f"""
                {region_config}
                {creds_config}
                {endpoint_config}
                {style_config}
            """
            console.print(f"[dim]Debug Setup SQL:\n{setup_sql}[/dim]")
            con.execute(setup_sql)

        return con
    except Exception as e:
        print_error(f"Failed to initialize DuckDB: {e}")
        return None


def run_query(config: BackparqConfig, sql: str) -> None:
    """
    Execute SQL query against S3 archives.

    Auto-registers tables in the config as views.
    View name = table name (safe).
    """
    con = _setup_duckdb(config)
    if not con:
        sys.exit(1)

    # Auto-register views for configured tables
    if config.s3.bucket:
        base_s3 = f"s3://{config.s3.bucket}/{config.s3.prefix}/archive"

        for table in config.archive.tables:
            # Handle schema.table -> schema_table
            valid_view_name = table.name.replace(".", "_")
            # Glob pattern for all parquet files
            s3_glob = f"{base_s3}/{table.name.replace('.', '_')}/**/*.parquet"

            console.print(f"[dim]Debug Glob: {s3_glob}[/dim]")

            try:
                con.execute(
                    f"CREATE OR REPLACE VIEW {valid_view_name} AS SELECT * FROM read_parquet('{s3_glob}');"
                )
                logger.debug(f"Registered view {valid_view_name} -> {s3_glob}")
            except Exception as e:
                logger.warning(f"Could not register view for {table.name}: {e}")
    else:
        # Local mode (less common for this command but good for consistency)
        pass

    console.print(f"[bold]Executing:[/bold] {sql}")
    console.print()

    try:
        # Run query and stream result to console
        # fetchdf() is good for display with rich/pandas, but simple fetchall is safer dep-wise
        # Let's try to use Rich table if we can, or just print
        result = con.execute(sql).fetchall()
        columns = [desc[0] for desc in con.description]

        from rich.table import Table

        table = Table(show_header=True, header_style="bold magenta")
        for col in columns:
            table.add_column(col)

        for row in result:
            table.add_row(*[str(x) for x in row])

        console.print(table)
        console.print(f"\n[dim]Returned {len(result)} rows[/dim]")

    except Exception as e:
        print_error(f"Query Execution Error: {e}")
        sys.exit(1)
