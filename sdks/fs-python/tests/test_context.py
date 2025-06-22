"""
Unit tests for the FSContext class.
"""

from unittest.mock import Mock

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
