import argparse
import sys
from pathlib import Path

from backparq.archive import archive_tables
from backparq.config import ConfigError, BackparqConfig, load_config
from backparq.cron import install_cron
from backparq.db import test_pg_connection
from backparq.parquet import build_encryption_properties
from backparq.s3 import test_s3_connection


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


def handle_test(args: argparse.Namespace) -> None:
    config = _load_config(args.config)
    run_tests(config)
    print("Config, database, and S3 connections validated successfully.")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="backparq")
    subparsers = parser.add_subparsers(dest="command", required=True)

    apply_parser = subparsers.add_parser("apply", help="Run archive and install cron if configured")
    apply_parser.add_argument("--config", required=True, help="Path to YAML config")
    apply_parser.set_defaults(func=handle_apply)

    test_parser = subparsers.add_parser("test", help="Validate config and test connections")
    test_parser.add_argument("--config", required=True, help="Path to YAML config")
    test_parser.set_defaults(func=handle_test)

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
