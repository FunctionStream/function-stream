"""
Unit tests for the FSFunction class.
"""

import pytest
import json
import asyncio
from unittest.mock import Mock, patch, AsyncMock
from fs_sdk.function import FSFunction
from fs_sdk.config import Config, PulsarConfig, ModuleConfig, SinkSpec, SourceSpec
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
            max_concurrent_requests=10,
            max_producer_cache_size=100
        )
        config.sources = [SourceSpec(pulsar={"topic": "test_topic"})]
        config.requestSource = SourceSpec(pulsar={"topic": "request_topic"})
        config.sink = SinkSpec(pulsar={"topic": "response_topic"})
        config.modules = ModuleConfig(active_module="test_module")
        return config

    @pytest.fixture
    def mock_process_func(self):
        """Create a mock process function."""
        async def process(data):
            return {"result": "test_result"}
        return Mock(wraps=process)

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
    def function(self, mock_config, mock_process_func, mock_client, mock_consumer, 
                mock_producer, mock_metrics, mock_metrics_server):
        """Create a FSFunction instance with mocks, patching Config to avoid file IO."""
        with patch('fs_sdk.function.Config.from_yaml', return_value=mock_config), \
             patch('fs_sdk.function.Client', return_value=mock_client), \
             patch('fs_sdk.function.Metrics', return_value=mock_metrics), \
             patch('fs_sdk.function.MetricsServer', return_value=mock_metrics_server):
            
            mock_client.subscribe.return_value = mock_consumer
            mock_client.create_producer.return_value = mock_producer
            
            function = FSFunction(
                process_funcs={"test_module": mock_process_func},
                config_path="test_config.yaml"
            )
            return function

    @pytest.mark.asyncio
    async def test_init(self, mock_config, mock_process_func):
        """Test FSFunction initialization."""
        with patch('fs_sdk.function.Config.from_yaml', return_value=mock_config), \
             patch('fs_sdk.function.Client'), \
             patch('fs_sdk.function.Metrics'), \
             patch('fs_sdk.function.MetricsServer'):
            
            function = FSFunction(
                process_funcs={"test_module": mock_process_func},
                config_path="test_config.yaml"
            )
            
            assert function.process_funcs["test_module"] == mock_process_func

    @pytest.mark.asyncio
    async def test_process_request_success(self, function, mock_process_func, mock_producer):
        """Test successful request processing."""
        # Setup
        message = Mock()
        message.data.return_value = json.dumps({"test": "data"}).encode('utf-8')
        message.properties.return_value = {
            "request_id": "test_id",
            "response_topic": "response_topic"
        }
        
        # Execute
        await function.process_request(message)
        
        # Verify
        mock_process_func.assert_called_once_with({"test": "data"})
        mock_producer.send.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_request_json_error(self, function, mock_metrics):
        """Test request processing with JSON decode error."""
        # Setup
        message = Mock()
        message.data.return_value = b"invalid json"
        
        # Execute
        await function.process_request(message)
        
        # Verify
        mock_metrics.record_event.assert_called_with(False)

    @pytest.mark.asyncio
    async def test_process_request_no_response_topic(self, function, mock_metrics):
        """Test request processing with no response topic."""
        # Setup
        message = Mock()
        message.data.return_value = json.dumps({"test": "data"}).encode('utf-8')
        message.properties.return_value = {"request_id": "test_id"}
        function.config.sink = None
        
        # Execute
        await function.process_request(message)
        
        # Verify
        mock_metrics.record_event.assert_called_with(False)

    @pytest.mark.asyncio
    async def test_start_and_shutdown(self, function, mock_consumer, mock_metrics_server):
        """Test function start and graceful shutdown."""
        import asyncio
        mock_consumer.receive.side_effect = [
            Mock(data=lambda: json.dumps({"test": "data"}).encode('utf-8'),
                 properties=lambda: {"request_id": "test_id", "response_topic": "response_topic"}),
            asyncio.CancelledError()
        ]
        # Execute
        try:
            await function.start()
        except asyncio.CancelledError:
            pass
        # Verify
        mock_metrics_server.start.assert_called_once()
        mock_metrics_server.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_graceful_shutdown(self, function, mock_consumer):
        """Test graceful shutdown process."""
        # Setup
        task = asyncio.create_task(asyncio.sleep(1))
        await function._add_task(task)
        
        # Execute
        await function._graceful_shutdown()
        
        # Verify
        assert task.cancelled()

    def test_get_metrics(self, function, mock_metrics):
        """Test metrics retrieval."""
        # Setup
        mock_metrics.get_metrics.return_value = {"test": "metrics"}
        
        # Execute
        result = function.get_metrics()
        
        # Verify
        mock_metrics.get_metrics.assert_called_once()
        assert result == {"test": "metrics"}

    @pytest.mark.asyncio
    async def test_send_response(self, function, mock_producer):
        """Test response sending."""
        # Setup
        response_topic = "test_topic"
        request_id = "test_id"
        response_data = {"result": "test"}
        
        # Execute
        await function._send_response(response_topic, request_id, response_data)
        
        # Verify
        mock_producer.send.assert_called_once_with(
            json.dumps(response_data).encode('utf-8'),
            properties={'request_id': request_id}
        )

    @pytest.mark.asyncio
    async def test_send_response_error(self, function, mock_producer):
        """Test response sending with error."""
        # Setup
        mock_producer.send.side_effect = Exception("Send error")
        
        # Execute and Verify
        with pytest.raises(Exception):
            await function._send_response("test_topic", "test_id", {"test": "data"}) 