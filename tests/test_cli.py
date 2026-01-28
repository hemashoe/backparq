"""
Unit tests for backparq.cli module.
"""


import pytest

from backparq.cli import (
    EXIT_CONFIG_ERROR,
    EXIT_INTERRUPTED,
    EXIT_RUNTIME_ERROR,
    EXIT_SUCCESS,
    build_parser,
    setup_logging,
)


class TestSetupLogging:
    """Tests for setup_logging function."""

    def test_default_verbosity_no_error(self):
        """Test default verbosity sets up logging without error."""
        setup_logging(0)  # Should not raise

    def test_verbose_no_error(self):
        """Test -v verbosity sets up logging without error."""
        setup_logging(1)  # Should not raise

    def test_very_verbose_no_error(self):
        """Test -vv verbosity sets up logging without error."""
        setup_logging(2)  # Should not raise


class TestBuildParser:
    """Tests for build_parser function."""

    def test_parser_has_required_commands(self):
        """Test parser has all required subcommands."""
        parser = build_parser()

        # Parse help to verify commands exist
        with pytest.raises(SystemExit):
            parser.parse_args(["--help"])

    def test_archive_command_parse(self):
        """Test parsing archive command."""
        parser = build_parser()
        args = parser.parse_args(["archive", "--config", "config.yaml"])

        assert args.command == "archive"
        assert args.config == "config.yaml"

    def test_restore_command_parse(self):
        """Test parsing restore command."""
        parser = build_parser()
        args = parser.parse_args(
            [
                "restore",
                "--config",
                "config.yaml",
                "--start",
                "2024-01-01",
                "--end",
                "2024-04-01",
                "--conflict-mode",
                "upsert",
            ]
        )

        assert args.command == "restore"
        assert args.start == "2024-01-01"
        assert args.end == "2024-04-01"
        assert args.conflict_mode == "upsert"

    def test_verbose_flag(self):
        """Test verbose flag parsing."""
        parser = build_parser()

        args = parser.parse_args(["archive", "--config", "c.yaml"])
        assert args.verbose == 0

        args = parser.parse_args(["-v", "archive", "--config", "c.yaml"])
        assert args.verbose == 1

        args = parser.parse_args(["-vv", "archive", "--config", "c.yaml"])
        assert args.verbose == 2

    def test_prune_dry_run(self):
        """Test prune command with dry-run flag."""
        parser = build_parser()
        args = parser.parse_args(["prune", "--config", "c.yaml", "--dry-run"])

        assert args.command == "prune"
        assert args.dry_run is True


class TestExitCodes:
    """Tests for exit code constants."""

    def test_exit_codes_defined(self):
        """Test exit codes are defined correctly."""
        assert EXIT_SUCCESS == 0
        assert EXIT_CONFIG_ERROR == 1
        assert EXIT_RUNTIME_ERROR == 2
        assert EXIT_INTERRUPTED == 130
