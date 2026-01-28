"""
Pytest configuration and fixtures for backparq tests.
"""

import sys
from pathlib import Path

import pytest

# Ensure src is in path for imports
src_path = Path(__file__).parent.parent / "src"
if str(src_path) not in sys.path:
    sys.path.insert(0, str(src_path))


# Prevent pytest from collecting test_s3_connection from the s3 module
def pytest_configure(config):
    """Configure pytest to ignore specific function names in source files."""
    pass


# Tell pytest to ignore functions named test_* in the source directory
collect_ignore_glob = []


@pytest.fixture
def sample_config_dict():
    """Return a sample configuration dictionary for testing."""
    return {
        "database": {
            "host": "localhost",
            "port": 5432,
            "name": "testdb",
            "user": "postgres",
            "password": "secret",
        },
        "s3": {
            "bucket": "test-bucket",
            "prefix": "archives",
            "region": "us-east-1",
        },
        "parquet": {
            "compression": "snappy",
            "encryption": {"enabled": False},
        },
        "archive": {
            "tables": ["public.events"],
            "mode": "offload",
            "concurrency": 1,
            "chunk_concurrency": 1,
            "dry_run": True,
        },
    }


@pytest.fixture
def temp_config_file(tmp_path, sample_config_dict):
    """Create a temporary config file for testing."""
    import yaml

    config_path = tmp_path / "config.yaml"
    with open(config_path, "w") as f:
        yaml.dump(sample_config_dict, f)

    return config_path
