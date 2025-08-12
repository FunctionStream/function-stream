"""
FunctionStream SDK - A Python SDK for building and deploying serverless functions.

This module provides the core functionality for creating and managing FunctionStream
functions. It handles message processing, request/response flow, and resource management.
The module includes classes for function execution, message handling, and Pulsar integration.
"""
import asyncio
import dataclasses
import functools
import inspect
import json
import logging
import os
import time
import typing
from datetime import datetime, timezone
from typing import Callable, Any, Dict, Set, Union, Awaitable, get_type_hints, List, Optional

import pulsar
from pulsar import Client, Producer

from .config import Config
from .context import FSContext
from .metrics import Metrics, MetricsServer
from .module import FSModule

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def _validate_process_func(func: Callable, module_name: str):
    """
    Validate the structure of a process function.

    This function performs comprehensive validation of a process function to ensure
    it meets the requirements for FunctionStream processing. It checks parameter
    count, types, and return types including support for async functions.

    Args:
        func (Callable): The function to validate
        module_name (str): Name of the module for error messages

    Raises:
        ValueError: If the function structure is invalid, including:
                   - Incorrect number of parameters
                   - Missing type hints
                   - Invalid parameter types
                   - Invalid return types
    """
    # Get function signature
    sig = inspect.signature(func)
    params = list(sig.parameters.values())

    # Check number of parameters
    if len(params) != 2:
        raise ValueError(
            f"Process function for module '{module_name}' must have exactly 2 parameters, "
            f"got {len(params)}"
        )

    # Check parameter types using type hints
    type_hints = get_type_hints(func)
    if not ("context" in type_hints and "data" in type_hints and "return" in type_hints):
        raise ValueError(
            f"Process function for module '{module_name}' must have type hints for both parameters named 'context', 'data', and a return type"
        )

    def unwrap_annotated(annotation):
        """Helper function to unwrap Annotated types."""
        origin = typing.get_origin(annotation)
        if origin is typing.Annotated:
            return unwrap_annotated(typing.get_args(annotation)[0])
        return annotation

    def is_dict_str_any(annotation):
        """Check if annotation represents Dict[str, Any] or dict[str, Any]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        return (origin in (dict, typing.Dict)) and args == (str, Any)

    if not (type_hints["context"] == FSContext):
        raise ValueError(
            f"Process function for module '{module_name}' must have FSContext as first parameter"
        )
    if not is_dict_str_any(type_hints["data"]):
        raise ValueError(
            f"Process function for module '{module_name}' must have Dict[str, Any] or dict[str, Any] as second parameter"
        )
    # Check return type
    return_type = type_hints.get('return')

    def is_dict_return(annotation):
        """Check if annotation represents Dict[str, Any] or dict[str, Any]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        return (origin in (dict, typing.Dict)) and args == (str, Any)

    def is_none_type(annotation):
        """Check if annotation represents None type."""
        ann = unwrap_annotated(annotation)
        return ann is type(None)

    def is_awaitable_dict(annotation):
        """Check if annotation represents Awaitable[Dict[str, Any]]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        return origin in (typing.Awaitable,) and len(args) == 1 and is_dict_return(args[0])

    def is_awaitable_none(annotation):
        """Check if annotation represents Awaitable[None]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        return origin in (typing.Awaitable,) and len(args) == 1 and is_none_type(args[0])

    def is_union_of_dict_and_none(annotation):
        """Check if annotation represents Union[Dict[str, Any], None]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        if origin in (typing.Union, Union):
            return (any(is_dict_return(arg) for arg in args) and any(is_none_type(arg) for arg in args))
        return False

    def is_awaitable_union_dict_none(annotation):
        """Check if annotation represents Awaitable[Union[Dict[str, Any], None]]."""
        ann = unwrap_annotated(annotation)
        origin = typing.get_origin(ann)
        args = typing.get_args(ann)
        if origin in (typing.Awaitable,):
            if len(args) == 1:
                return is_union_of_dict_and_none(args[0])
        return False

    if not (
            is_dict_return(return_type)
            or is_awaitable_dict(return_type)
            or is_none_type(return_type)
            or is_awaitable_none(return_type)
            or is_union_of_dict_and_none(return_type)
            or is_awaitable_union_dict_none(return_type)
    ):
        raise ValueError(
            f"Process function for module '{module_name}' must return Dict[str, Any], dict[str, Any], None, Awaitable thereof, or a Union with None, got {return_type}"
        )


@dataclasses.dataclass
class MsgWrapper:
    """
    Wrapper class for message data and event timing.
    
    This class encapsulates message data along with its associated event timestamp,
    providing a structured way to handle messages throughout the processing pipeline.
    
    Attributes:
        data (Dict[str, Any]): The message data payload.
        event_time (Optional[datetime]): The timestamp when the event occurred.
    """
    data: Dict[str, Any]
    event_time: Optional[datetime] = None


class FSFunction:
    """
    FunctionStream Function - A serverless function handler for processing messages.

    This class provides a framework for building serverless functions that can process
    messages from multiple Pulsar topics. It handles message consumption, processing,
    and response generation, while managing resources and providing metrics.

    The FSFunction class is the main entry point for creating FunctionStream functions.
    It manages the entire lifecycle of message processing, including:
    - Pulsar client and consumer setup
    - Message processing with configurable concurrency limits
    - Response handling and error management
    - Metrics collection and monitoring
    - Graceful shutdown and resource cleanup

    Attributes:
        config (Config): Configuration object containing function settings.
        process_funcs (Dict[str, Union[Callable, FSModule]]): Dictionary of process functions or modules by module name.
        client (Client): Pulsar client instance for message consumption and production.
        semaphore (asyncio.Semaphore): Semaphore for controlling concurrent requests.
        metrics (Metrics): Metrics collection object for monitoring function performance.
        metrics_server (MetricsServer): Server for exposing metrics via HTTP endpoint.
        context (FSContext): Context object for accessing configuration and runtime information.
        _shutdown_event (asyncio.Event): Event flag for graceful shutdown coordination.
        _current_tasks (Set[asyncio.Task]): Set of currently running processing tasks.
        _tasks_lock (asyncio.Lock): Lock for thread-safe task management.
        _consumer: Pulsar consumer for message consumption.
    """

    def __init__(
            self,
            process_funcs: Dict[
                str, Union[Callable[
                    ["FSContext", Dict[str, Any]], Union[Dict[str, Any], Awaitable[Dict[str, Any]]]], FSModule]],
            config_path: str = None
    ):
        """
        Initialize the FS Function.

        This method sets up the FunctionStream function with the provided process functions
        and configuration. It performs validation of the module configuration and sets up
        the Pulsar client, consumer, and other resources needed for message processing.

        Args:
            process_funcs (Dict[str, Union[Callable, FSModule]]): Dictionary mapping module names to their process functions or modules.
                Each function must accept two parameters: (context: FSContext, data: Dict[str, Any])
                and return either a Dict[str, Any] or an Awaitable[Dict[str, Any]].
                Each module must be an instance of FSModule.
            config_path (str): Path to the configuration file. If None, uses FS_CONFIG_PATH environment variable or defaults to "config.yaml".

        Raises:
            ValueError: If no module is specified in config or if the specified module
                      doesn't have a corresponding process function, or if the function
                      structure is invalid.
            Exception: If there are errors during Pulsar client setup or consumer creation.
        """
        if config_path is None:
            config_path = os.getenv("FS_CONFIG_PATH", "config.yaml")
        self.config = Config.from_yaml(config_path)
        self.process_funcs = process_funcs
        self.context = FSContext(self.config)

        # Validate module
        module = self.config.module
        if not module:
            raise ValueError("No module specified in config")
        if module not in process_funcs:
            raise ValueError(f"Process function not found for module: {module}")

        # Validate function structure
        process_func = process_funcs[module]
        if isinstance(process_func, FSModule):
            # For FSModule, we'll use its process method
            process_func.init(self.context)
        else:
            _validate_process_func(process_func, module)

        # Create authentication if specified
        auth = None
        if self.config.pulsar.authPlugin:
            auth = pulsar.Authentication(
                self.config.pulsar.authPlugin,
                self.config.pulsar.authParams
            )

        self.client = Client(
            self.config.pulsar.serviceUrl,
            authentication=auth,
            operation_timeout_seconds=30
        )
        self.semaphore = asyncio.Semaphore(self.config.pulsar.max_concurrent_requests)
        self.metrics = Metrics()
        self.metrics_server = MetricsServer(self.metrics, port=self.config.metric.port)
        self._shutdown_event = asyncio.Event()
        self._current_tasks: Set[asyncio.Task] = set()
        self._tasks_lock = asyncio.Lock()
        self._consumer = None

        # Create multi-topics consumer
        self._setup_consumer()

    def _setup_consumer(self):
        """
        Set up a multi-topics consumer for all sources and the request source.

        This method creates a Pulsar consumer that subscribes to multiple topics
        specified in the configuration. It collects topics from both regular sources
        and the request source, creating a single consumer that can handle messages
        from all configured topics.

        The consumer is configured with shared subscription type and appropriate
        timeout settings for non-ordering guarantee workloads.

        Raises:
            ValueError: If no subscription name is set or if no valid sources are found.
        """
        topics = []
        subscription_name = self.config.subscriptionName

        if not subscription_name:
            raise ValueError("subscriptionName is not set in config.yaml")

        # Collect topics from sources
        for source in self.config.sources:
            if source.pulsar and source.pulsar.topic:
                topics.append(source.pulsar.topic)
                logger.info(f"Added source topic: {source.pulsar.topic}")

        # Collect topics from request sources
        if self.config.requestSource and self.config.requestSource.pulsar and self.config.requestSource.pulsar.topic:
            topics.append(self.config.requestSource.pulsar.topic)
            logger.info(f"Added request source topic: {self.config.requestSource.pulsar.topic}")

        if not topics:
            raise ValueError("No valid sources or request sources found in config")

        # Create multi-topics consumer
        self._consumer = self.client.subscribe(
            topics,
            subscription_name,
            consumer_type=pulsar.ConsumerType.Shared,
            unacked_messages_timeout_ms=30_000  # Only for non-ordering guarantee workload
        )
        logger.info(f"Created multi-topics consumer for topics: {topics} with subscription: {subscription_name}")

    async def _add_task(self, task: asyncio.Task):
        """
        Thread-safe method to add a task to the tracking set.

        This method safely adds a task to the internal tracking set using a lock
        to ensure thread safety when multiple tasks are being processed concurrently.

        Args:
            task (asyncio.Task): The task to add to tracking.
        """
        async with self._tasks_lock:
            self._current_tasks.add(task)

    async def _remove_task(self, task: asyncio.Task):
        """
        Thread-safe method to remove a task from the tracking set.

        This method safely removes a task from the internal tracking set using a lock
        to ensure thread safety. It handles any exceptions that might occur during
        the removal process.

        Args:
            task (asyncio.Task): The task to remove from tracking.
        """
        async with self._tasks_lock:
            try:
                self._current_tasks.discard(task)
            except Exception as e:
                logger.error(f"Error removing task: {str(e)}")

    async def _get_tasks(self) -> Set[asyncio.Task]:
        """
        Thread-safe method to get a copy of current tasks.

        This method returns a copy of the current tasks set to avoid race conditions
        when iterating over the tasks. The copy is made while holding the lock.

        Returns:
            Set[asyncio.Task]: A copy of the current tasks set.
        """
        async with self._tasks_lock:
            return set(self._current_tasks)

    @functools.lru_cache(maxsize=100)
    def _get_producer(self, topic: str) -> Producer:
        """
        Get a producer for the specified topic.

        This method uses an LRU cache to efficiently manage Pulsar producers.
        Producers are cached by topic to avoid creating new ones for each message,
        improving performance and resource utilization.

        Args:
            topic (str): The topic to create a producer for.

        Returns:
            Producer: A Pulsar producer for the specified topic.
        """
        return self.client.create_producer(topic)

    async def process_request(self, message):
        """
        Process an incoming request and send a response.

        This method is the core message processing function that:
        1. Records metrics for the request
        2. Processes the request using the configured module
        3. Sends the response back to the appropriate topic
        4. Handles any errors that occur during processing
        5. Manages message acknowledgment

        The method supports both synchronous and asynchronous process functions,
        and handles various types of responses including error responses.

        Args:
            message: The incoming Pulsar message to process.
        """
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
                    if not response_topic and self.config.sink and self.config.sink.pulsar and self.config.sink.pulsar.topic:
                        response_topic = self.config.sink.pulsar.topic

                    module = self.config.module
                    process_func = self.process_funcs[module]

                    context = FSContext(self.config)
                    resp_msgs: List[MsgWrapper] = []

                    def produce(data: Dict[str, Any], event_time: datetime = None):
                        """Local produce function to collect response messages."""
                        resp_msgs.append(MsgWrapper(data=data, event_time=event_time))

                    context.produce = produce

                    def get_metadata(key: str) -> Any:
                        """Local metadata function to provide message metadata."""
                        if key == "topic":
                            return message.topic_name()
                        if key == "message_id":
                            return message.message_id()
                        raise KeyError(key)

                    context.get_metadata = get_metadata

                    # Call the function with context as first argument and handle both sync and async results
                    response_data = None
                    try:
                        if isinstance(process_func, FSModule):
                            result = process_func.process(context, request_data)
                        else:
                            result = process_func(context, request_data)

                        if result is not None:
                            if isinstance(result, Awaitable):
                                response_data = await result
                            else:
                                response_data = result
                    except Exception as e:
                        logger.error(f"Error invoking process function: {str(e)}")
                        raise Exception(f"Error invoking process function: {str(e)}") from e
                    if response_data:
                        resp_msgs.append(MsgWrapper(data=response_data, event_time=datetime.now(timezone.utc)))

                    if not response_topic:
                        logger.warning("No response_topic provided and no sink topic available. Skip messages")
                    else:
                        await self._send_response(response_topic, request_id, resp_msgs)

                    latency = time.time() - start_time
                    self.metrics.record_request_end(True, latency)
                    self.metrics.record_event(True)

                    if request_id is None:
                        logger.info(f"Finished processing request and acknowledged {message.message_id()}")
                        self._consumer.acknowledge(message)

                except json.JSONDecodeError as e:
                    logger.error(f"Failed to decode request JSON: {e}")
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
                    raise e
                except asyncio.CancelledError as e:
                    logger.info("Request processing cancelled due to shutdown")
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
                    raise e
                except Exception as e:
                    logger.error(f"Error processing request: {type(e).__name__}: {e}")
                    if not self._shutdown_event.is_set():
                        if request_id:  # Only send the response back if the request_id exists
                            await self._send_response(
                                response_topic,
                                request_id,
                                [MsgWrapper(data={'error': str(e)}, event_time=datetime.now(timezone.utc))]
                            )
                    self.metrics.record_request_end(False, time.time() - start_time)
                    self.metrics.record_event(False)
        finally:
            await self._remove_task(task)
            if request_id:
                self._consumer.acknowledge(message)

    async def _send_response(self, response_topic: str, request_id: str, msg: List[MsgWrapper]):
        """
        Send a response message using cached producer asynchronously.

        This method sends response messages to the specified topic using the cached
        Pulsar producer. It handles multiple messages in parallel and provides
        proper error handling and logging for failed sends.

        The method converts datetime objects to ISO format strings for JSON serialization
        and sets appropriate event timestamps for Pulsar messages.

        Args:
            response_topic (str): The topic to send the response to.
            request_id (str): The ID of the request being responded to.
            msg (List[MsgWrapper]): The list of messages to send.

        Raises:
            Exception: If there's an error sending the response.
        """
        loop = asyncio.get_event_loop()
        try:
            producer = self._get_producer(response_topic)

            def default_serializer(o):
                """Custom JSON serializer for datetime objects."""
                if isinstance(o, datetime):
                    return o.isoformat()
                return str(o)

            send_futures = []
            for m in msg:
                future = loop.create_future()
                message_data = json.dumps(m.data, default=default_serializer).encode('utf-8')

                def create_callback(f):
                    """Create a callback function for async message sending."""
                    def callback(res, msg_id):
                        if res != pulsar.Result.Ok:
                            err = Exception(f"Error producing: {res}")
                            logger.error(str(err))
                            loop.call_soon_threadsafe(f.set_exception, err)
                        else:
                            loop.call_soon_threadsafe(f.set_result, msg_id)

                    return callback

                event_timestamp = None
                if m.event_time is not None:
                    # Convert datetime to milliseconds since epoch, with exact millisecond precision
                    event_timestamp = int(
                        m.event_time.replace(tzinfo=timezone.utc).timestamp()) * 1000 + m.event_time.microsecond // 1000
                send_kwargs = dict(
                    event_timestamp=event_timestamp
                )
                if request_id is not None:
                    send_kwargs['properties'] = {'request_id': request_id}
                producer.send_async(
                    message_data,
                    create_callback(future),
                    **send_kwargs
                )
                send_futures.append(future)
            await asyncio.gather(*send_futures, return_exceptions=True)
        except Exception as e:
            logger.error(f"Error sending response: {type(e).__name__}: {e}")
            raise

    async def start(self):
        """
        Start processing requests from all consumers.

        This method is the main entry point for starting the FunctionStream function.
        It:
        1. Starts the metrics server for monitoring
        2. Enters a loop to process incoming messages
        3. Handles graceful shutdown when requested
        4. Manages the consumer receive loop with proper error handling

        The method runs indefinitely until a shutdown signal is received, either
        through cancellation or keyboard interrupt.
        """
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
                        asyncio.create_task(self.process_request(msg))
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
        """
        Close the service and clean up resources.

        This method performs a graceful shutdown of the FunctionStream function by:
        1. Stopping the metrics server
        2. Closing the Pulsar consumer
        3. Clearing the producer cache
        4. Closing the Pulsar client

        The method ensures that all resources are properly cleaned up and handles
        any errors that might occur during the shutdown process.
        """
        logger.info("Closing FS Function resources...")

        await self.metrics_server.stop()

        # Close consumer
        if self._consumer is not None:
            try:
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
        """
        Ensure resources are cleaned up when the object is destroyed.

        This finalizer ensures that all resources are properly closed when the
        object is garbage collected. It provides a safety net for resource cleanup
        in case the explicit close() method is not called.
        """
        if self._consumer is not None:
            try:
                self._consumer.close()
            except:
                pass
        try:
            self._get_producer.cache_clear()
        except:
            pass
        if self.client is not None:
            try:
                self.client.close()
            except:
                pass

    def get_metrics(self) -> Dict[str, Any]:
        """
        Get current metrics for monitoring.

        This method returns the current metrics collected by the FunctionStream function,
        providing insights into performance, throughput, and error rates.

        Returns:
            Dict[str, Any]: A dictionary containing the current metrics.
        """
        return self.metrics.get_metrics()

    def get_context(self) -> FSContext:
        """
        Get the FSContext instance associated with this function.

        This method provides access to the context object that contains configuration
        and runtime information for the function.

        Returns:
            FSContext: The context object containing configuration and runtime information.
        """
        return self.context
