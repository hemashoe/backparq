"""
Unit tests for backparq.storage.s3 module.
"""

from unittest.mock import MagicMock, patch

import botocore.exceptions
import pytest

from backparq.storage.s3 import (
    verify_checksum as s3_verify_object_sha256,
)
from backparq.storage.s3 import (
    verify_connection as verify_s3_connection,
)


class TestS3VerifyObjectSha256:
    """Tests for s3_verify_object_sha256 function."""

    def test_matching_checksum_returns_true(self):
        """Test matching checksum returns True."""
        s3 = MagicMock()
        s3.head_object.return_value = {"Metadata": {"sha256": "abc123"}}

        result = s3_verify_object_sha256(s3, "bucket", "key", "abc123")
        assert result is True

    def test_mismatched_checksum_returns_false(self):
        """Test mismatched checksum returns False."""
        s3 = MagicMock()
        s3.head_object.return_value = {"Metadata": {"sha256": "different"}}

        result = s3_verify_object_sha256(s3, "bucket", "key", "expected")
        assert result is False

    def test_missing_object_returns_false(self):
        """Test missing object returns False."""
        s3 = MagicMock()
        error_response = {"Error": {"Code": "404"}}
        s3.head_object.side_effect = botocore.exceptions.ClientError(error_response, "HeadObject")

        result = s3_verify_object_sha256(s3, "bucket", "key", "sha")
        assert result is False

    def test_missing_metadata_returns_false(self):
        """Test missing metadata key returns False."""
        s3 = MagicMock()
        s3.head_object.return_value = {"Metadata": {}}

        result = s3_verify_object_sha256(s3, "bucket", "key", "expected")
        assert result is False


class TestVerifyS3Connection:
    """Tests for verify_s3_connection function."""

    def test_successful_connection(self):
        """Test successful S3 connection."""
        config = MagicMock()
        config.bucket = "test-bucket"

        with patch("backparq.storage.s3.create_client") as mock_client:
            mock_s3 = MagicMock()
            mock_client.return_value = mock_s3

            verify_s3_connection(config)  # Should not raise
            mock_s3.head_bucket.assert_called_once_with(Bucket="test-bucket")

    def test_connection_failure_raises(self):
        """Test connection failure raises RuntimeError."""
        config = MagicMock()
        config.bucket = "test-bucket"

        with patch("backparq.storage.s3.create_client") as mock_client:
            mock_s3 = MagicMock()
            error_response = {"Error": {"Code": "403"}}
            mock_s3.head_bucket.side_effect = botocore.exceptions.ClientError(
                error_response, "HeadBucket"
            )
            mock_client.return_value = mock_s3

            with pytest.raises(RuntimeError, match="not accessible"):
                verify_s3_connection(config)
