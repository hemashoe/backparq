import argparse
import sys
from pathlib import Path

from backparq.archive import archive_tables
from backparq.config import ConfigError, BackparqConfig, load_config, parse_utc_datetime
from backparq.cron import install_cron
from backparq.db import test_pg_connection
from backparq.parquet import build_encryption_properties
from backparq.s3 import test_s3_connection
from backparq.restore import restore_tables
from backparq.check import check_backups
from backparq.prune import prune_backups


def handle_check(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    check_backups(config) 

def handle_prune(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    prune_backups(config, dry_run=args.dry_run)


def _load_config(path_str: str) -> BackparqConfig:
    try:
        return load_config(Path(path_str))
    except ConfigError as exc:
        raise SystemExit(f"Config error: {exc}") from exc


def run_tests(config: BackparqConfig) -> None:
    build_encryption_properties(config.parquet)
    test_pg_connection(config.database)
    test_s3_connection(config.s3)


def handle_apply(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    run_tests(config)

    if config.cron.enabled:
        install_cron(config, Path(args.config))

    archive_tables(config)


def handle_archive(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    run_tests(config)
    archive_tables(config)


def handle_test(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    run_tests(config)
    print("Config, database, and S3 connections validated successfully.")


def handle_restore(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    
    start = parse_utc_datetime(args.start)
    end = parse_utc_datetime(args.end)
    
    # Allow overriding tables via CLI? Maybe later. For now use config tables.
    
    restore_tables(
        config=config,
        start_date=start,
        end_date=end,
        dry_run=args.dry_run,
        conflict_mode=args.conflict_mode,
        backup_id=args.backup_id,
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="backparq")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply", help="Run archive and install cron if configured")
    apply_parser.add_argument("--config", required=True, help="Path to YAML config")
    apply_parser.set_defaults(func=handle_apply)

    archive_parser = subparsers.add_parser("archive", help="Run one-off archive (backup) immediately")
    archive_parser.add_argument("--config", required=True, help="Path to YAML config")
    archive_parser.set_defaults(func=handle_archive)

    test_parser = subparsers.add_parser("test", help="Validate config and test connections")
    test_parser.add_argument("--config", required=True, help="Path to YAML config")
    test_parser.set_defaults(func=handle_test)

    restore_parser = subparsers.add_parser("restore", help="Restore archived data to Postgres")
    restore_parser.add_argument("--config", required=True, help="Path to YAML config")
    restore_parser.add_argument("--start", required=True, help="Start date (ISO8601, inclusive)")
    restore_parser.add_argument("--end", required=True, help="End date (ISO8601, exclusive)")
    restore_parser.add_argument("--conflict-mode", choices=["do_nothing", "upsert"], default="do_nothing", help="Conflict resolution strategy")
    restore_parser.add_argument("--backup-id", help="If restoring from backup mode, specify run ID (e.g. 2025-01-01_120000)")
    restore_parser.add_argument("--dry-run", action="store_true", help="Simulate restore without writing")
    restore_parser.set_defaults(func=handle_restore)

    check_parser = subparsers.add_parser("check", help="List and check backups in S3")
    check_parser.add_argument("--config", required=True, help="Path to YAML config")
    check_parser.set_defaults(func=handle_check)

    prune_parser = subparsers.add_parser("prune", help="Delete old backups based on retention policy")
    prune_parser.add_argument("--config", required=True, help="Path to YAML config")
    prune_parser.add_argument("--dry-run", action="store_true", help="Simulate prune without deleting")
    prune_parser.set_defaults(func=handle_prune)

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    try:
        args.func(args)
    except SystemExit:
        raise
    except Exception as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
