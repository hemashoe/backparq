"""CLI for backparq."""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

from backparq.archive import archive_tables
from backparq.check import check_backups
from backparq.config import BackparqConfig, ConfigError, load_config, parse_utc_datetime
from backparq.db import test_pg_connection
from backparq.plan import plan_archive
from backparq.prune import prune_backups
from backparq.restore import restore_tables
from backparq.status import show_status
from backparq.storage.parquet import build_encryption
from backparq.storage.s3 import verify_connection as verify_s3_connection
from backparq.utils.console import console, print_error, print_info, print_success, print_warning
from backparq.verify import verify_archives

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_RUNTIME_ERROR = 2
EXIT_INTERRUPTED = 130


from backparq.utils.logging import setup_logging


def _load_config(path_str: str) -> BackparqConfig:
    try:
        config = load_config(Path(path_str))
        logger.debug(f"Loaded config from: {path_str}")
        return config
    except ConfigError as exc:
        print_error(f"Config error: {exc}")
        raise SystemExit(EXIT_CONFIG_ERROR) from exc


def run_tests(config: BackparqConfig) -> None:
    build_encryption(config.parquet)
    test_pg_connection(config.database)
    if config.s3.bucket:
        verify_s3_connection(config.s3)


def handle_test(args):
    config = _load_config(args.config)
    run_tests(config)
    print_success("All connections validated")


import signal
import threading


def handle_archive(args):
    config = _load_config(args.config)
    run_tests(config)

    shutdown_event = threading.Event()

    def signal_handler(sig, frame):
        logger.warning(f"Received signal {sig}, initiating graceful shutdown...")
        shutdown_event.set()

    original_sigint = signal.getsignal(signal.SIGINT)
    original_sigterm = signal.getsignal(signal.SIGTERM)

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        result = archive_tables(config, show_stats=args.stats, shutdown_event=shutdown_event)
        if args.output == "json":
            print(json.dumps(result.to_dict(), indent=2))

        if not result.success:
            sys.exit(EXIT_RUNTIME_ERROR)
    finally:
        # Restore original handlers
        signal.signal(signal.SIGINT, original_sigint)
        signal.signal(signal.SIGTERM, original_sigterm)


def handle_apply(args):
    """Legacy command - just runs archive."""
    config = _load_config(args.config)
    run_tests(config)
    archive_tables(config)


def handle_restore(args):
    config = _load_config(args.config)
    try:
        start, end = parse_utc_datetime(args.start), parse_utc_datetime(args.end)
    except ConfigError as e:
        print_error(f"Invalid date: {e}")
        raise SystemExit(EXIT_CONFIG_ERROR) from e
    restore_tables(config, start, end, args.dry_run, args.conflict_mode, args.backup_id)


def handle_check(args):
    check_backups(_load_config(args.config))


def handle_prune(args):
    prune_backups(_load_config(args.config), dry_run=args.dry_run)


def handle_plan(args):
    plan = plan_archive(_load_config(args.config))
    print(json.dumps(plan, indent=2, default=str))


def handle_status(args):
    show_status(
        _load_config(args.config), table_filter=args.table, output_json=args.output == "json"
    )


def handle_verify(args):
    result = verify_archives(_load_config(args.config), repair=args.repair, table_filter=args.table)
    if args.output == "json":
        print(json.dumps(result.to_dict(), indent=2))
    if not result.success:
        sys.exit(EXIT_RUNTIME_ERROR)


def handle_validate(args):
    """Validate configuration and connections."""
    console.print("[bold]Validating configuration...[/bold]")
    try:
        config = _load_config(args.config)
        print_success(f"Config syntax valid: [cyan]{args.config}[/cyan]")

        # Test DB
        console.print(
            f"Testing connection to [cyan]{config.database.host}:{config.database.port}[/cyan]..."
        )
        test_pg_connection(config.database)
        print_success("Database connection successful")

        # Test S3
        if config.s3.bucket:
            console.print(f"Testing connection to S3 bucket [cyan]{config.s3.bucket}[/cyan]...")
            verify_s3_connection(config.s3)
            print_success("S3 connection successful")
        else:
            print_warning("S3 not configured (skippable for dry-run)")

        # Notifications check
        if config.notifications and config.notifications.enabled:
            print_info(f"Notifications enabled: {len(config.notifications.urls)} URLs")

        print_success("Configuration is valid and ready to use.")

    except Exception as e:
        print_error(f"Validation failed: {e}")
        sys.exit(EXIT_CONFIG_ERROR)


from backparq.query import run_query


def handle_query(args):
    """Run SQL query against archives."""
    config = _load_config(args.config)
    # Basic validation but no full test_pg_connection needed for S3 query
    if not config.s3.bucket:
        print_error("Query command requires S3 configuration.")
        sys.exit(EXIT_CONFIG_ERROR)

    run_query(config, args.sql)


def handle_init(args):
    from rich.prompt import Prompt

    from backparq.utils.console import console

    console.print("[bold]Backparq Configuration Generator[/bold]")
    console.print()

    config = {
        "database": {
            "host": Prompt.ask("Database host", default="localhost"),
            "port": int(Prompt.ask("Database port", default="5432")),
            "name": Prompt.ask("Database name"),
            "user": Prompt.ask("Database user", default="postgres"),
            "password": "${PG_PASSWORD}",
        },
        "s3": {
            "bucket": Prompt.ask("S3 bucket name"),
            "prefix": Prompt.ask("S3 prefix", default="backparq"),
            "region": Prompt.ask("AWS region", default="us-east-1"),
        },
        "archive": {
            "mode": Prompt.ask("Archive mode", choices=["offload", "backup"], default="offload"),
            "tables": [],
        },
    }

    while True:
        table = Prompt.ask("Table name (or 'done')")
        if table.lower() == "done":
            break
        config["archive"]["tables"].append(table)

    if not config["archive"]["tables"]:
        print_warning("No tables configured")
        return

    import yaml

    output_path = Path(args.output) if args.output else Path("backparq.yaml")
    with open(output_path, "w") as f:
        yaml.safe_dump(config, f, default_flow_style=False, sort_keys=False)

    print_success(f"Configuration written to {output_path}")


def build_parser() -> argparse.ArgumentParser:
    from backparq import __version__

    parser = argparse.ArgumentParser(
        prog="backparq",
        description="Archive PostgreSQL tables to Parquet files on S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  backparq validate --config config.yaml
  backparq archive --config config.yaml -v --stats
  backparq query --config config.yaml --sql "SELECT * FROM public_events LIMIT 10"
  backparq status --config config.yaml
  backparq restore --config config.yaml --start 2024-01-01 --end 2024-04-01
""",
    )
    parser.add_argument(
        "-V", "--version", action="version", version=f"%(prog)s {__version__}"
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Verbosity (-v INFO, -vv DEBUG)"
    )
    parser.add_argument(
        "-q", "--quiet", action="store_true", help="Suppress progress output"
    )
    parser.add_argument(
        "--log-format",
        choices=["text", "json"],
        default="text",
        help="Log format (text or json for structured logs)",
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("validate", help="Validate config and connections")
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_validate)

    p = sub.add_parser("query", help="Run SQL query on archives")
    p.add_argument("--config", required=True)
    p.add_argument("--sql", required=True, help="SQL query (DuckDB)")
    p.set_defaults(func=handle_query)

    p = sub.add_parser("test", help="Test connections")
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_test)

    p = sub.add_parser("archive", help="Archive tables to Parquet/S3")
    p.add_argument("--config", required=True)
    p.add_argument("--stats", action="store_true", help="Show statistics")
    p.add_argument("--output", choices=["text", "json"], default="text")
    p.set_defaults(func=handle_archive)

    # Deprecated: kept for backward compatibility but hidden from help
    p = sub.add_parser("apply", help=argparse.SUPPRESS)
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_apply)

    p = sub.add_parser("restore", help="Restore from archive")
    p.add_argument("--config", required=True)
    p.add_argument("--start", required=True, help="Start date (ISO8601)")
    p.add_argument("--end", required=True, help="End date (ISO8601)")
    p.add_argument("--conflict-mode", choices=["do_nothing", "upsert"], default="do_nothing")
    p.add_argument("--backup-id", help="Backup snapshot ID")
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=handle_restore)

    p = sub.add_parser("check", help="List S3 backups")
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_check)

    p = sub.add_parser("prune", help="Delete old backups")
    p.add_argument("--config", required=True)
    p.add_argument("--dry-run", action="store_true")
    p.set_defaults(func=handle_prune)

    p = sub.add_parser("plan", help="Generate archive plan (JSON)")
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_plan)

    p = sub.add_parser("status", help="Show archive status")
    p.add_argument("--config", required=True)
    p.add_argument("--table", help="Filter table")
    p.add_argument("--output", choices=["text", "json"], default="text")
    p.set_defaults(func=handle_status)

    p = sub.add_parser("verify", help="Verify archive integrity")
    p.add_argument("--config", required=True)
    p.add_argument("--repair", action="store_true")
    p.add_argument("--table", help="Filter table")
    p.add_argument("--output", choices=["text", "json"], default="text")
    p.set_defaults(func=handle_verify)

    p = sub.add_parser("init", help="Generate config file")
    p.add_argument("--output", "-o", help="Output path")
    p.set_defaults(func=handle_init)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    setup_logging(args.verbose, log_format=getattr(args, "log_format", "text"))

    try:
        args.func(args)
    except KeyboardInterrupt:
        print_warning("Interrupted")
        sys.exit(EXIT_INTERRUPTED)
    except Exception as exc:
        if args.verbose >= 2:
            import traceback

            traceback.print_exc()
        print_error(str(exc))
        sys.exit(EXIT_RUNTIME_ERROR)


if __name__ == "__main__":
    main()
