from unittest.mock import MagicMock, patch

import pytest

from backparq.cli import build_parser, handle_query


def test_query_command_args():
    parser = build_parser()
    args = parser.parse_args(["query", "--config", "config.yaml", "--sql", "SELECT 1"])
    assert args.command == "query"
    assert args.config == "config.yaml"
    assert args.sql == "SELECT 1"
    assert args.func == handle_query


@patch("backparq.cli.run_query")
@patch("backparq.cli._load_config")
def test_handle_query_calls_implementation(mock_load, mock_run):
    # Mock config
    mock_config = MagicMock()
    mock_config.s3.bucket = "my-bucket"
    mock_load.return_value = mock_config

    parser = build_parser()
    args = parser.parse_args(["query", "--config", "config.yaml", "--sql", "SELECT * FROM table"])

    # Execute handler
    args.func(args)

    # Verify calls
    mock_load.assert_called_with("config.yaml")
    mock_run.assert_called_with(mock_config, "SELECT * FROM table")


@patch("backparq.cli._load_config")
def test_handle_query_no_s3_bucket(mock_load):
    mock_config = MagicMock()
    mock_config.s3.bucket = None
    mock_load.return_value = mock_config

    parser = build_parser()
    args = parser.parse_args(["query", "--config", "config.yaml", "--sql", "SELECT 1"])

    with pytest.raises(SystemExit) as exc:
        args.func(args)
    assert exc.value.code == 1
