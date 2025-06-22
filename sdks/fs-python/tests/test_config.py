"""
Unit tests for the Config class.
"""

import pytest
import yaml

from function_stream import Config


class TestConfig:
    """Test suite for Config class."""

    @pytest.fixture
    def sample_config_yaml(self, tmp_path):
        """Create a sample config.yaml file for testing."""
        config_data = {
            "pulsar": {
                "serviceUrl": "pulsar://localhost:6650",
                "authPlugin": "",
                "authParams": "",
                "max_concurrent_requests": 10
            },
            "module": "test_module",
            "sources": [
                {
                    "pulsar": {
                        "topic": "test_topic"
                    }
                }
            ],
            "requestSource": {
                "pulsar": {
                    "topic": "request_topic"
                }
            },
            "sink": {
                "pulsar": {
                    "topic": "response_topic"
                }
            },
            "subscriptionName": "test_subscription",
            "name": "test_function",
            "description": "Test function",
            "config": {
                "test_key": "test_value"
            }
        }

        config_path = tmp_path / "config.yaml"
        with open(config_path, 'w') as f:
            yaml.dump(config_data, f)
        return str(config_path)

    def test_from_yaml(self, sample_config_yaml):
        """Test loading configuration from YAML file."""
        config = Config.from_yaml(sample_config_yaml)

        # Test Pulsar config
        assert config.pulsar.serviceUrl == "pulsar://localhost:6650"
        assert config.pulsar.authPlugin == ""
        assert config.pulsar.authParams == ""
        assert config.pulsar.max_concurrent_requests == 10

        # Test module config
        assert config.module == "test_module"

        # Test sources
        assert len(config.sources) == 1
        assert config.sources[0].pulsar.topic == "test_topic"

        # Test request source
        assert config.requestSource.pulsar.topic == "request_topic"

        # Test sink
        assert config.sink.pulsar.topic == "response_topic"

        # Test subscription name
        assert config.subscriptionName == "test_subscription"

        # Test name and description
        assert config.name == "test_function"
        assert config.description == "Test function"

        # Test config values
        assert config.get_config_value("test_key") == "test_value"

    def test_from_yaml_file_not_found(self):
        """Test loading configuration from non-existent file."""
        with pytest.raises(FileNotFoundError):
            Config.from_yaml("non_existent.yaml")

    def test_get_config_value_not_found(self, sample_config_yaml):
        """Test getting non-existent config value."""
        config = Config.from_yaml(sample_config_yaml)
        assert config.get_config_value("non_existent_key") is None
