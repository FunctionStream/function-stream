import logging
import time
from typing import Dict, Any

from aiohttp import web

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Metrics:
    """
    Prometheus-style metrics for monitoring system performance.
    
    This class tracks various metrics including request counts, latencies, and event statistics.
    All metrics are exposed in Prometheus-compatible format.
    """

    def __init__(self):
        self.total_requests = 0
        self.active_requests = 0
        self.successful_requests = 0
        self.failed_requests = 0
        self.request_latency = 0.0
        self.last_request_time = 0.0
        self.total_events = 0
        self.successful_events = 0
        self.failed_events = 0

    def record_request_start(self):
        """
        Record the start of a new request.
        
        This method increments the total request counter and active request counter,
        and updates the last request timestamp.
        """
        self.total_requests += 1
        self.active_requests += 1
        self.last_request_time = time.time()

    def record_request_end(self, success: bool, latency: float):
        """
        Record the end of a request.
        
        Args:
            success (bool): Whether the request was successful.
            latency (float): The request latency in seconds.
        """
        self.active_requests -= 1
        if success:
            self.successful_requests += 1
        else:
            self.failed_requests += 1
        self.request_latency = latency

    def record_event(self, success: bool):
        """
        Record an event (success or failure).
        
        Args:
            success (bool): Whether the event was successful.
        """
        self.total_events += 1
        if success:
            self.successful_events += 1
        else:
            self.failed_events += 1

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics in Prometheus format.
        
        Returns:
            Dict[str, Any]: A dictionary containing all metrics in Prometheus-compatible format.
                           Includes request counts, latencies, event statistics, and derived metrics
                           like success rates.
        """
        return {
            # Request metrics
            'fs_total_requests': self.total_requests,
            'fs_active_requests': self.active_requests,
            'fs_successful_requests': self.successful_requests,
            'fs_failed_requests': self.failed_requests,
            'fs_request_latency_seconds': self.request_latency,
            'fs_last_request_timestamp': self.last_request_time,

            # Event metrics
            'fs_total_events': self.total_events,
            'fs_successful_events': self.successful_events,
            'fs_failed_events': self.failed_events,

            # Derived metrics
            'fs_request_success_rate': (
                        self.successful_requests / self.total_requests) if self.total_requests > 0 else 0,
            'fs_event_success_rate': (self.successful_events / self.total_events) if self.total_events > 0 else 0
        }


class MetricsServer:
    def __init__(self, metrics: Metrics, host: str = '127.0.0.1', port: int = 9099):
        self.metrics = metrics
        self.host = host
        self.port = port
        self.app = web.Application()
        self.app.router.add_get('/', self.handle_root)
        self.app.router.add_get('/metrics', self.handle_metrics)
        self.runner = None

    async def handle_root(self, request):
        """Handle root endpoint request"""
        return web.Response(text="FS SDK Metrics Server\nUse /metrics endpoint to get metrics data")

    async def handle_metrics(self, request):
        """Handle metrics endpoint request"""
        try:
            metrics_data = self.metrics.get_metrics()
            return web.json_response(metrics_data)
        except Exception as e:
            logger.error(f"Error getting metrics: {str(e)}")
            return web.json_response(
                {"error": "Failed to get metrics"},
                status=500
            )

    async def start(self):
        """Start the metrics server"""
        try:
            self.runner = web.AppRunner(self.app)
            await self.runner.setup()
            site = web.TCPSite(self.runner, self.host, self.port)
            await site.start()
            logger.info(f"Metrics server started at http://{self.host}:{self.port}/metrics")
        except Exception as e:
            logger.error(f"Failed to start metrics server: {str(e)}")
            raise

    async def stop(self):
        """Stop the metrics server"""
        if self.runner:
            try:
                await self.runner.cleanup()
                logger.info("Metrics server stopped")
            except Exception as e:
                logger.error(f"Error stopping metrics server: {str(e)}")
                raise
