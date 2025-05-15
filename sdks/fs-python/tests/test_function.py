"""
Unit tests for the FSFunction class.
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from fs_sdk.function import FSFunction
from fs_sdk.config import Config, PulsarConfig, SinkSpec, SourceSpec, PulsarSourceConfig
from fs_sdk.metrics import Metrics, MetricsServer

class TestFSFunction:
    """Test suite for FSFunction class."""

    @pytest.fixture
    def mock_config(self):
        """Create a mock Config object for testing."""
        config = Mock(spec=Config)
        config.module = "test_module"
        config.subscriptionName = "test_subscription"
        config.pulsar = PulsarConfig(
            serviceUrl="pulsar://localhost:6650",
            authPlugin="",
            authParams="",
            max_concurrent_requests=10
        )
        config.sources = [SourceSpec(pulsar=PulsarSourceConfig(topic="test_topic"))]
        config.requestSource = SourceSpec(pulsar=PulsarSourceConfig(topic="request_topic"))
        config.sink = SinkSpec(pulsar=PulsarSourceConfig(topic="response_topic"))
        return config

    @pytest.fixture
    def mock_client(self):
        """Create a mock Pulsar client."""
        client = Mock()
        return client

    @pytest.fixture
    def mock_consumer(self):
        """Create a mock Pulsar consumer."""
        consumer = Mock()
        return consumer

    @pytest.fixture
    def mock_producer(self):
        """Create a mock Pulsar producer."""
        producer = Mock()
        return producer

    @pytest.fixture
    def mock_metrics(self):
        """Create a mock Metrics object."""
        metrics = Mock(spec=Metrics)
        return metrics

    @pytest.fixture
    def mock_metrics_server(self):
        """Create a mock MetricsServer object."""
        metrics_server = Mock(spec=MetricsServer)
        metrics_server.start = AsyncMock()
        metrics_server.stop = AsyncMock()
        return metrics_server

    @pytest.fixture
    def function(self, mock_config, mock_client, mock_consumer, 
                mock_producer, mock_metrics, mock_metrics_server):
        """Create a FSFunction instance with mocks, patching Config to avoid file IO."""
        with patch('fs_sdk.function.Config.from_yaml', return_value=mock_config), \
             patch('fs_sdk.function.Client', return_value=mock_client), \
             patch('fs_sdk.function.Metrics', return_value=mock_metrics), \
             patch('fs_sdk.function.MetricsServer', return_value=mock_metrics_server):
            
            mock_client.subscribe.return_value = mock_consumer
            mock_client.create_producer.return_value = mock_producer
            
            from fs_sdk.context import FSContext
            from typing import Dict, Any

            async def process_func(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
                return {"result": "test_result"}
            process_funcs = {"test_module": process_func}
            function = FSFunction(
                process_funcs=process_funcs,
                config_path="test_config.yaml"
            )
            return function

    @pytest.mark.asyncio
    async def test_init(self):
        """Test FSFunction initialization."""
        import inspect
        from fs_sdk.context import FSContext
        from typing import Dict, Any

        async def process_func(context: FSContext, data: Dict[str, Any]) -> Dict[str, Any]:
            return {"result": "test_result"}
        process_funcs = {"test_module": process_func}
        with patch('fs_sdk.function.Config.from_yaml') as mock_from_yaml, \
             patch('fs_sdk.function.Client'), \
             patch('fs_sdk.function.Metrics'), \
             patch('fs_sdk.function.MetricsServer'):
            mock_config = Mock()
            mock_config.module = "test_module"
            mock_config.subscriptionName = "test_subscription"
            class PulsarConfig:
                authPlugin = ""
                authParams = ""
                max_concurrent_requests = 10
                serviceUrl = "pulsar://localhost:6650"
            mock_config.pulsar = PulsarConfig()
            mock_config.sources = [SourceSpec(pulsar=PulsarSourceConfig(topic="test_topic"))]
            mock_config.requestSource = None
            mock_config.sink = None
            mock_from_yaml.return_value = mock_config
            function = FSFunction(
                process_funcs=process_funcs,
                config_path="test_config.yaml"
            )
            sig = inspect.signature(function.process_funcs["test_module"])
            assert list(sig.parameters.keys()) == ["context", "data"]

    @pytest.mark.asyncio
    async def test_process_request_success(self, function, mock_producer):
        """Test successful request processing."""
        message = Mock()
        message.data.return_value = json.dumps({"test": "data"}).encode('utf-8')
        message.properties.return_value = {
            "request_id": "test_id",
            "response_topic": "response_topic"
        }
        await function.process_request(message)
        mock_producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_request_json_error(self, function, mock_metrics):
        """Test request processing with JSON decode error."""
        message = Mock()
        message.data.return_value = b"invalid json"
        await function.process_request(message)
        mock_metrics.record_event.assert_called_with(False)

    @pytest.mark.asyncio
    async def test_process_request_no_response_topic(self, function, mock_metrics):
        """Test request processing with no response topic."""
        message = Mock()
        message.data.return_value = json.dumps({"test": "data"}).encode('utf-8')
        message.properties.return_value = {"request_id": "test_id"}
        function.config.sink = None
        await function.process_request(message)
        mock_metrics.record_event.assert_called_with(False)

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self, function, mock_consumer, mock_metrics_server):
        """Test function start and graceful shutdown."""
        mock_consumer.receive.side_effect = [
            Mock(data=lambda: json.dumps({"test": "data"}).encode('utf-8'),
                 properties=lambda: {"request_id": "test_id", "response_topic": "response_topic"}),
            asyncio.CancelledError()
        ]
        try:
            await function.start()
        except asyncio.CancelledError:
            pass
        mock_metrics_server.start.assert_called_once()
        mock_metrics_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, function, mock_consumer):
        """Test graceful shutdown process."""
        task = asyncio.create_task(asyncio.sleep(1))
        await function._add_task(task)
        await function._graceful_shutdown()
        assert task.cancelled()

    def test_get_metrics(self, function, mock_metrics):
        """Test metrics retrieval."""
        mock_metrics.get_metrics.return_value = {"test": "metrics"}
        result = function.get_metrics()
        mock_metrics.get_metrics.assert_called_once()
        assert result == {"test": "metrics"}

    def test_get_context(self, function, mock_config):
        """Test context retrieval."""
        context = function.get_context()
        assert context is not None
        assert context.config == mock_config

    @pytest.mark.asyncio
    async def test_send_response(self, function, mock_producer):
        """Test response sending."""
        response_topic = "test_topic"
        request_id = "test_id"
        response_data = {"result": "test"}
        await function._send_response(response_topic, request_id, response_data)
        mock_producer.send.assert_called_once_with(
            json.dumps(response_data).encode('utf-8'),
            properties={'request_id': request_id}
        )

    @pytest.mark.asyncio
    async def test_send_response_error(self, function, mock_producer):
        """Test response sending with error."""
        mock_producer.send.side_effect = Exception("Send error")
        with pytest.raises(Exception):
            await function._send_response("test_topic", "test_id", {"test": "data"}) 