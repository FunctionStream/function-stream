"""
Unit tests for the FSContext class.
"""

from unittest.mock import Mock
from datetime import datetime, timezone

import pytest

from function_stream import FSContext, Config


class TestFSContext:
    """Test suite for FSContext class."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock Config object for testing."""
        config = Mock(spec=Config)
        return config

    @pytest.fixture
    def context(self, mock_config):
        """Create a FSContext instance with mock config."""
        return FSContext(mock_config)

    def test_init(self, mock_config):
        """Test FSContext initialization."""
        context = FSContext(mock_config)
        assert context.config == mock_config

    def test_get_config_success(self, context, mock_config):
        """Test successful config value retrieval."""
        # Setup
        mock_config.get_config_value.return_value = "test_value"

        # Execute
        result = context.get_config("test_key")

        # Verify
        mock_config.get_config_value.assert_called_once_with("test_key")
        assert result == "test_value"

    def test_get_config_error(self, context, mock_config):
        """Test config value retrieval with error."""
        # Setup
        mock_config.get_config_value.side_effect = Exception("Test error")

        # Execute
        result = context.get_config("test_key")

        # Verify
        mock_config.get_config_value.assert_called_once_with("test_key")
        assert result == ""

    def test_get_config_non_string_value(self, context, mock_config):
        """Test config value retrieval with non-string value."""
        # Setup
        mock_config.get_config_value.return_value = 123

        # Execute
        result = context.get_config("test_key")

        # Verify
        mock_config.get_config_value.assert_called_once_with("test_key")
        assert result == 123

    def test_get_metadata_default_implementation(self, context):
        """Test that get_metadata returns None by default."""
        result = context.get_metadata("any_key")
        assert result is None

    def test_produce_default_implementation(self, context):
        """Test that produce does nothing by default."""
        test_data = {"key": "value"}
        test_time = datetime.now(timezone.utc)

        # Should not raise any exception
        result = context.produce(test_data, test_time)
        assert result is None

    def test_produce_without_event_time(self, context):
        """Test produce method without event_time parameter."""
        test_data = {"key": "value"}

        # Should not raise any exception
        result = context.produce(test_data)
        assert result is None

    def test_get_configs(self, context, mock_config):
        """Test get_configs method."""
        # Setup
        mock_config.config = {"key1": "value1", "key2": "value2"}

        # Execute
        result = context.get_configs()

        # Verify
        assert result == {"key1": "value1", "key2": "value2"}

    def test_get_module(self, context, mock_config):
        """Test get_module method."""
        # Setup
        mock_config.module = "test_module"

        # Execute
        result = context.get_module()

        # Verify
        assert result == "test_module"
