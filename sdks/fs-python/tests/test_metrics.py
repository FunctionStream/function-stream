"""
Unit tests for the Metrics and MetricsServer classes.
"""

import json

import pytest
from aiohttp.test_utils import make_mocked_request

from function_stream import Metrics, MetricsServer


@pytest.fixture
def metrics():
    return Metrics()


@pytest.fixture
def metrics_server(metrics):
    return MetricsServer(metrics, host='127.0.0.1', port=9099)


class TestMetrics:
    def test_initial_state(self, metrics):
        """Test initial state of metrics"""
        assert metrics.total_requests == 0
        assert metrics.active_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.failed_requests == 0
        assert metrics.request_latency == 0.0
        assert metrics.last_request_time == 0.0
        assert metrics.total_events == 0
        assert metrics.successful_events == 0
        assert metrics.failed_events == 0

    def test_record_request_start(self, metrics):
        """Test recording request start"""
        metrics.record_request_start()
        assert metrics.total_requests == 1
        assert metrics.active_requests == 1
        assert metrics.last_request_time > 0

    def test_record_request_end_success(self, metrics):
        """Test recording successful request end"""
        metrics.record_request_start()
        metrics.record_request_end(success=True, latency=0.5)
        assert metrics.active_requests == 0
        assert metrics.successful_requests == 1
        assert metrics.failed_requests == 0
        assert metrics.request_latency == 0.5

    def test_record_request_end_failure(self, metrics):
        """Test recording failed request end"""
        metrics.record_request_start()
        metrics.record_request_end(success=False, latency=0.5)
        assert metrics.active_requests == 0
        assert metrics.successful_requests == 0
        assert metrics.failed_requests == 1
        assert metrics.request_latency == 0.5

    def test_record_event_success(self, metrics):
        """Test recording successful event"""
        metrics.record_event(success=True)
        assert metrics.total_events == 1
        assert metrics.successful_events == 1
        assert metrics.failed_events == 0

    def test_record_event_failure(self, metrics):
        """Test recording failed event"""
        metrics.record_event(success=False)
        assert metrics.total_events == 1
        assert metrics.successful_events == 0
        assert metrics.failed_events == 1

    def test_get_metrics_empty(self, metrics):
        """Test getting metrics when no data has been recorded"""
        metrics_data = metrics.get_metrics()
        assert metrics_data['fs_total_requests'] == 0
        assert metrics_data['fs_active_requests'] == 0
        assert metrics_data['fs_successful_requests'] == 0
        assert metrics_data['fs_failed_requests'] == 0
        assert metrics_data['fs_request_latency_seconds'] == 0.0
        assert metrics_data['fs_total_events'] == 0
        assert metrics_data['fs_successful_events'] == 0
        assert metrics_data['fs_failed_events'] == 0
        assert metrics_data['fs_request_success_rate'] == 0
        assert metrics_data['fs_event_success_rate'] == 0

    def test_get_metrics_with_data(self, metrics):
        """Test getting metrics with recorded data"""
        # Record some requests
        metrics.record_request_start()
        metrics.record_request_end(success=True, latency=0.5)
        metrics.record_request_start()
        metrics.record_request_end(success=False, latency=0.3)

        # Record some events
        metrics.record_event(success=True)
        metrics.record_event(success=True)
        metrics.record_event(success=False)

        metrics_data = metrics.get_metrics()
        assert metrics_data['fs_total_requests'] == 2
        assert metrics_data['fs_active_requests'] == 0
        assert metrics_data['fs_successful_requests'] == 1
        assert metrics_data['fs_failed_requests'] == 1
        assert metrics_data['fs_request_latency_seconds'] == 0.3
        assert metrics_data['fs_total_events'] == 3
        assert metrics_data['fs_successful_events'] == 2
        assert metrics_data['fs_failed_events'] == 1
        assert metrics_data['fs_request_success_rate'] == 0.5
        assert metrics_data['fs_event_success_rate'] == 2 / 3


@pytest.mark.asyncio
class TestMetricsServer:
    async def test_handle_root(self, metrics_server):
        """Test root endpoint handler"""
        request = make_mocked_request('GET', '/')
        response = await metrics_server.handle_root(request)
        assert response.status == 200
        text = response.text
        assert "FS SDK Metrics Server" in text

    async def test_handle_metrics_empty(self, metrics_server):
        """Test metrics endpoint with no data"""
        request = make_mocked_request('GET', '/metrics')
        response = await metrics_server.handle_metrics(request)
        assert response.status == 200
        data = json.loads(response.text)
        assert data['fs_total_requests'] == 0
        assert data['fs_active_requests'] == 0

    async def test_handle_metrics_with_data(self, metrics_server):
        """Test metrics endpoint with recorded data"""
        # Record some data
        metrics_server.metrics.record_request_start()
        metrics_server.metrics.record_request_end(success=True, latency=0.5)
        metrics_server.metrics.record_event(success=True)

        request = make_mocked_request('GET', '/metrics')
        response = await metrics_server.handle_metrics(request)
        assert response.status == 200
        data = json.loads(response.text)
        assert data['fs_total_requests'] == 1
        assert data['fs_successful_requests'] == 1
        assert data['fs_total_events'] == 1
        assert data['fs_successful_events'] == 1

    async def test_server_start_stop(self, metrics_server):
        """Test starting and stopping the metrics server"""
        # Start server
        await metrics_server.start()
        assert metrics_server.runner is not None

        # Stop server
        await metrics_server.stop()
        # Note: runner is not set to None after cleanup in aiohttp
        # We just verify that the server was started and stopped successfully
