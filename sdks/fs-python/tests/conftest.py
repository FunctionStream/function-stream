"""
Shared test configurations and fixtures.
"""

from unittest.mock import Mock

import pytest


@pytest.fixture
def mock_pulsar_message():
    """Create a mock Pulsar message."""

    def create_message(data, properties=None):
        message = Mock()
        message.data.return_value = data
        message.properties.return_value = properties or {}
        return message

    return create_message
