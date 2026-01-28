"""CLI for backparq."""

import argparse
import json
import logging
import sys
from pathlib import Path

from backparq.archive import archive_tables
from backparq.check import check_backups
from backparq.config import BackparqConfig, ConfigError, load_config, parse_utc_datetime
from backparq.console import print_error, print_success, print_warning
from backparq.cron import install_cron
from backparq.db import test_pg_connection
from backparq.parquet import build_encryption_properties
from backparq.prune import prune_backups
from backparq.restore import restore_tables
from backparq.s3 import verify_s3_connection
from backparq.status import show_status
from backparq.verify import verify_archives

logger = logging.getLogger(__name__)

EXIT_SUCCESS = 0
EXIT_CONFIG_ERROR = 1
EXIT_RUNTIME_ERROR = 2
EXIT_INTERRUPTED = 130


def setup_logging(verbosity: int = 0) -> None:
    if verbosity >= 2:
        level, fmt = logging.DEBUG, "%(asctime)s %(levelname)-8s [%(name)s:%(lineno)d] %(message)s"
    elif verbosity >= 1:
        level, fmt = logging.INFO, "%(asctime)s %(levelname)-8s %(message)s"
    else:
        level, fmt = logging.WARNING, "%(levelname)-8s %(message)s"

    logging.basicConfig(
        level=level,
        format=fmt,
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stderr)],
    )
    for name in ["boto3", "botocore", "urllib3"]:
        logging.getLogger(name).setLevel(logging.WARNING)


def _load_config(path_str: str) -> BackparqConfig:
    try:
        return load_config(Path(path_str))
    except ConfigError as exc:
        print_error(f"Config error: {exc}")
        raise SystemExit(EXIT_CONFIG_ERROR) from exc


def run_tests(config: BackparqConfig) -> None:
    build_encryption_properties(config.parquet)
    test_pg_connection(config.database)
    if config.s3.bucket:
        verify_s3_connection(config.s3)


def handle_test(args):
    config = _load_config(args.config)
    run_tests(config)
    print_success("All connections validated")


def handle_archive(args):
    config = _load_config(args.config)
    run_tests(config)
    result = archive_tables(config, show_stats=args.stats)
    if args.output == "json":
        print(json.dumps(result.to_dict(), indent=2))
    if not result.success:
        sys.exit(EXIT_RUNTIME_ERROR)


def handle_apply(args):
    config = _load_config(args.config)
    run_tests(config)
    if config.cron.enabled:
        install_cron(config, Path(args.config))
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


def handle_init(args):
    from rich.prompt import Prompt

    from backparq.console import console

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
    parser = argparse.ArgumentParser(
        prog="backparq",
        description="Archive PostgreSQL tables to Parquet files on S3",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  backparq test --config config.yaml
  backparq archive --config config.yaml -v --stats
  backparq status --config config.yaml
  backparq restore --config config.yaml --start 2024-01-01 --end 2024-04-01
""",
    )
    parser.add_argument(
        "-v", "--verbose", action="count", default=0, help="Verbosity (-v INFO, -vv DEBUG)"
    )
    sub = parser.add_subparsers(dest="command", required=True)

    p = sub.add_parser("test", help="Test connections")
    p.add_argument("--config", required=True)
    p.set_defaults(func=handle_test)

    p = sub.add_parser("archive", help="Archive tables to Parquet/S3")
    p.add_argument("--config", required=True)
    p.add_argument("--stats", action="store_true", help="Show statistics")
    p.add_argument("--output", choices=["text", "json"], default="text")
    p.set_defaults(func=handle_archive)

    p = sub.add_parser("apply", help="Archive and install cron")
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
    setup_logging(args.verbose)

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
