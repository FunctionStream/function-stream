"""
Shared test configurations and fixtures.
"""

import pytest
import asyncio
from unittest.mock import Mock

@pytest.fixture(scope="session")
def event_loop():
    """Create an instance of the default event loop for each test case."""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()

@pytest.fixture
def mock_pulsar_message():
    """Create a mock Pulsar message."""
    def create_message(data, properties=None):
        message = Mock()
        message.data.return_value = data
        message.properties.return_value = properties or {}
        return message
    return create_message 