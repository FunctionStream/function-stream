import json
import asyncio
import logging
import pulsar
import time
import functools
import signal
from typing import Callable, Any, Dict, Optional, Set, List
from pulsar import Client, Consumer, Producer
from .config import Config
from .metrics import Metrics, MetricsServer

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FSContext:
    """
    Context class that provides access to configuration values.
    """
    def __init__(self, config: Config):
        self.config = config

    def get_config(self, config_name: str) -> str:
        """
        Get a configuration value by name.

        Args:
            config_name (str): The name of the configuration to retrieve

        Returns:
            str: The configuration value, or empty string if not found
        """
        try:
            return str(self.config.get_config_value(config_name))
        except Exception as e:
            logger.error(f"Error getting config {config_name}: {str(e)}")
            return ""

class FSFunction:
    """
    FS SDK - A simple RPC service handler that allows users to focus on their core business logic.
    Supports multiple modules with a single active module at runtime.
    """

    def __init__(
        self,
        process_funcs: Dict[str, Callable[[Dict[str, Any]], Dict[str, Any]]],
        config_path: str = "config.yaml"
    ):
        """
        Initialize the FS Function.

        Args:
            process_funcs (Dict[str, Callable]): Dictionary mapping module names to their process functions
            config_path (str): Path to the configuration file
        """
        self.config = Config(config_path)
        self.process_funcs = process_funcs
        
        # Create authentication if specified
        auth = None
        if self.config.auth_plugin:
            auth = pulsar.Authentication(
                self.config.auth_plugin,
                self.config.auth_params
            )
            
        self.client = Client(
            self.config.service_url,
            authentication=auth,
            operation_timeout_seconds=30
        )
        self.semaphore = asyncio.Semaphore(10)  # Default max concurrent requests
        self.metrics = Metrics()
        self.metrics_server = MetricsServer(self.metrics)
        self._shutdown_event = asyncio.Event()
        self._current_tasks: Set[asyncio.Task] = set()
        self._tasks_lock = asyncio.Lock()
        self._consumer = None
        self.context = FSContext(self.config)
        
        # Validate module
        module = self.config.module
        if not module:
            raise ValueError("No module specified in config")
        if module not in process_funcs:
            raise ValueError(f"Process function not found for module: {module}")
        
        # Create multi-topics consumer
        self._setup_consumer()

    def _setup_consumer(self):
        """Set up a multi-topics consumer for all sources and request sources."""
        topics = []
        subscription_name = self.config.subscription_name

        if not subscription_name:
            raise ValueError("subscription_name is not set in config.yaml")

        # Collect topics from sources
        for source in self.config.sources:
            pulsar_config = self.config.get_pulsar_source_config(source)
            if pulsar_config:
                topic = pulsar_config.get('topic')
                if topic:
                    topics.append(topic)
                    logger.info(f"Added source topic: {topic}")

        # Collect topics from request sources
        for source in self.config.request_sources:
            pulsar_config = self.config.get_pulsar_source_config(source)
            if pulsar_config:
                topic = pulsar_config.get('topic')
                if topic:
                    topics.append(topic)
                    logger.info(f"Added request source topic: {topic}")

        if not topics:
            raise ValueError("No valid sources or request sources found in config")

        # Create multi-topics consumer
        self._consumer = self.client.subscribe(
            topics,
            subscription_name,
            consumer_type=pulsar.ConsumerType.Shared
        )
        logger.info(f"Created multi-topics consumer for topics: {topics} with subscription: {subscription_name}")

    def _signal_handler(self, signum, frame):
        """Handle termination signals"""
        logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        asyncio.create_task(self._graceful_shutdown())

    async def _add_task(self, task: asyncio.Task):
        """Thread-safe method to add a task to the tracking set"""
        async with self._tasks_lock:
            self._current_tasks.add(task)

    async def _remove_task(self, task: asyncio.Task):
        """Thread-safe method to remove a task from the tracking set"""
        async with self._tasks_lock:
            try:
                self._current_tasks.discard(task)
            except Exception as e:
                logger.error(f"Error removing task: {str(e)}")

    async def _get_tasks(self) -> Set[asyncio.Task]:
        """Thread-safe method to get a copy of current tasks"""
        async with self._tasks_lock:
            return set(self._current_tasks)

    async def _graceful_shutdown(self):
        """Perform graceful shutdown of the service"""
        logger.info("Starting graceful shutdown...")
        self._shutdown_event.set()
        
        tasks_to_cancel = await self._get_tasks()
        
        if tasks_to_cancel:
            logger.info(f"Cancelling {len(tasks_to_cancel)} ongoing tasks...")
            for task in tasks_to_cancel:
                if not task.done():
                    task.cancel()
            
            try:
                await asyncio.gather(*tasks_to_cancel, return_exceptions=True)
            except Exception as e:
                logger.error(f"Error while cancelling tasks: {str(e)}")
            logger.info("All ongoing tasks cancelled")
        
        await self.close()
        logger.info("Graceful shutdown completed")

    @functools.lru_cache(maxsize=100)
    def _get_producer(self, topic: str) -> Producer:
        """Get a producer for the specified topic."""
        return self.client.create_producer(topic)

    async def process_request(self, message):
        """Process an incoming request and send a response."""
        start_time = time.time()
        self.metrics.record_request_start()
        
        task = asyncio.current_task()
        await self._add_task(task)
        
        try:
            async with self.semaphore:
                if self._shutdown_event.is_set():
                    logger.info("Skipping request processing due to shutdown")
                    return

                try:
                    request_data = json.loads(message.data().decode('utf-8'))
                    request_id = message.properties().get('request_id')
                    response_topic = message.properties().get('response_topic')

                    # If no response_topic is provided, use the sink topic as default
                    if not response_topic and self.config.sinks:
                        sink_config = self.config.get_pulsar_sink_config(self.config.sinks[0])
                        if sink_config and 'topic' in sink_config:
                            response_topic = sink_config['topic']
                            logger.info(f"Using sink topic as default response topic: {response_topic}")

                    if not response_topic:
                        logger.error("No response_topic provided and no sink topic available")
                        self.metrics.record_event(False)
                        return

                    module = self.config.module
                    process_func = self.process_funcs[module]

                    response_data = await process_func(self.context, request_data)

                    if self._shutdown_event.is_set():
                        logger.info("Skipping response sending due to shutdown")
                        return

                    await self._send_response(response_topic, request_id, response_data)
                    
                    latency = time.time() - start_time
                    self.metrics.record_request_end(True, latency)
                    self.metrics.record_event(True)

                except json.JSONDecodeError:
                    logger.error("Failed to decode request JSON")
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
                except asyncio.CancelledError:
                    logger.info("Request processing cancelled due to shutdown")
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
                    raise
                except Exception as e:
                    logger.error(f"Error processing request: {str(e)}")
                    if not self._shutdown_event.is_set():
                        await self._send_response(
                            response_topic,
                            request_id,
                            {'error': str(e)}
                        )
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
        finally:
            await self._remove_task(task)

    async def _send_response(self, response_topic: str, request_id: str, response_data: dict):
        """Send a response message using cached producer."""
        try:
            producer = self._get_producer(response_topic)
            message_data = json.dumps(response_data).encode('utf-8')
            producer.send(
                message_data,
                properties={'request_id': request_id}
            )
        except Exception as e:
            logger.error(f"Error sending response: {str(e)}")
            raise

    async def start(self):
        """Start processing requests from all consumers."""
        module = self.config.module
        logger.info(f"Starting FS Function with module: {module}")
        
        await self.metrics_server.start()
        
        try:
            while not self._shutdown_event.is_set():
                try:
                    msg = await asyncio.get_event_loop().run_in_executor(
                        None, lambda: self._consumer.receive(1000)
                    )
                    if msg:
                        await self.process_request(msg)
                        self._consumer.acknowledge(msg)
                except pulsar.Timeout:
                    continue
                except asyncio.CancelledError:
                    logger.info("Received cancellation signal, initiating shutdown...")
                    self._shutdown_event.set()
                    break
                except Exception as e:
                    logger.error(f"Error in request processing loop: {str(e)}")
                    if not self._shutdown_event.is_set():
                        await asyncio.sleep(1)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, initiating shutdown...")
            self._shutdown_event.set()
        finally:
            logger.info("Request processing loop stopped")
            await self.close()

    async def close(self):
        """Close the service and clean up resources."""
        logger.info("Closing FS Function resources...")
        
        await self.metrics_server.stop()
        
        # Close consumer
        if self._consumer is not None:
            try:
                self._consumer.unsubscribe()
                self._consumer.close()
                self._consumer = None
                logger.info("Consumer closed successfully")
            except Exception as e:
                logger.error(f"Error closing consumer: {str(e)}")
        
        # Clear the producer cache
        self._get_producer.cache_clear()
        
        # Close the Pulsar client
        try:
            await asyncio.sleep(0.1)
            self.client.close()
            logger.info("Pulsar client closed successfully")
        except Exception as e:
            if "AlreadyClosed" not in str(e):
                logger.error(f"Error closing Pulsar client: {str(e)}")

    def __del__(self):
        """Ensure resources are cleaned up when the object is destroyed."""
        if self._consumer is not None:
            try:
                self._consumer.close()
            except:
                pass
        try:
            self._get_producer.cache_clear()
        except:
            pass
        if hasattr(self, 'client'):
            try:
                self.client.close()
            except:
                pass

    def get_metrics(self) -> Dict[str, Any]:
        """Get current metrics for monitoring."""
        return self.metrics.get_metrics() 