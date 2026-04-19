# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Docker-managed Kafka broker for integration tests.

Provides automated image pull, idempotent container start, health check,
topic lifecycle management, and data cleanup via KRaft-mode single-node Kafka.
"""

import logging
import time
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TypeVar

import docker
from docker.errors import APIError, DockerException, NotFound
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka import TopicPartition as _new_topic_partition
from confluent_kafka.admin import AdminClient, NewTopic

from .utils import find_free_port

logger = logging.getLogger(__name__)

T = TypeVar("T")


class KafkaDockerManagerError(Exception):
    """Base exception for KafkaDockerManager errors."""
    pass


class KafkaReadinessTimeoutError(KafkaDockerManagerError):
    """Raised when Kafka fails to become ready within the timeout."""
    pass


@dataclass(frozen=True)
class KafkaConfig:
    """Configuration for the Kafka Docker Container."""
    image: str = "apache/kafka:3.7.0"
    container_name: str = "fs-integration-kafka-broker"
    bootstrap_servers: str = "127.0.0.1:9092"
    internal_port: int = 9092
    controller_port: int = 9093
    cluster_id: str = "fs-integration-test-cluster-01"
    readiness_timeout_sec: int = 60

    @property
    def environment_vars(self) -> Dict[str, str]:
        """Generate KRaft environment variables."""
        return {
            "KAFKA_NODE_ID": "1",
            "KAFKA_PROCESS_ROLES": "broker,controller",
            "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
            "KAFKA_LISTENERS": (
                f"PLAINTEXT://0.0.0.0:{self.internal_port},"
                f"CONTROLLER://0.0.0.0:{self.controller_port}"
            ),
            "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
            ),
            "KAFKA_ADVERTISED_LISTENERS": f"PLAINTEXT://{self.bootstrap_servers}",
            "KAFKA_CONTROLLER_QUORUM_VOTERS": f"1@localhost:{self.controller_port}",
            "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
            "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
            "CLUSTER_ID": self.cluster_id,
        }


class KafkaDockerManager:
    """
    Manages a single-node Kafka broker inside a Docker container (KRaft mode).

    Designed to be stateless with respect to topics. Highly recommended to use
    as a context manager to ensure proper cleanup.

    Usage::

        with KafkaDockerManager() as mgr:
            mgr.create_topics_if_not_exist(["input-topic", "output-topic"])
            # Run tests...
            mgr.clear_all_topics()
    """

    def __init__(
            self,
            config: Optional[KafkaConfig] = None,
            docker_client: Optional[docker.DockerClient] = None,
    ) -> None:
        if config is None:
            host_port = find_free_port()
            config = KafkaConfig(bootstrap_servers=f"127.0.0.1:{host_port}")
        self.config = config
        # Dependency Injection: Allow passing an existing client, or create a lazy one.
        self._docker_client = docker_client

    @property
    def docker_client(self) -> docker.DockerClient:
        """Lazy initialization of the Docker client."""
        if self._docker_client is None:
            try:
                self._docker_client = docker.from_env()
            except DockerException as e:
                raise KafkaDockerManagerError(f"Failed to connect to Docker daemon: {e}") from e
        return self._docker_client

    # ------------------------------------------------------------------
    # Context Manager Protocol
    # ------------------------------------------------------------------

    def __enter__(self) -> "KafkaDockerManager":
        self.setup_kafka()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        self.teardown_kafka()

    # ------------------------------------------------------------------
    # Full setup / teardown
    # ------------------------------------------------------------------

    def setup_kafka(self) -> None:
        """Pull image -> start container -> wait for readiness."""
        logger.info("Setting up Kafka broker (KRaft)...")
        self._ensure_image()
        self._ensure_container()
        self._wait_for_readiness()
        logger.info("Kafka setup complete. Broker is ready.")

    def teardown_kafka(self) -> None:
        """Stop and gracefully remove the Kafka container."""
        try:
            container = self.docker_client.containers.get(self.config.container_name)
            logger.info("Stopping Kafka container '%s'...", self.config.container_name)
            container.stop(timeout=5)  # Give Kafka a few seconds for graceful shutdown
        except NotFound:
            logger.debug("Container '%s' not found during teardown.", self.config.container_name)
        except APIError as exc:
            logger.warning("Docker API error while stopping Kafka: %s", exc)
        except Exception as exc:
            logger.error("Unexpected error during teardown: %s", exc)

    # ------------------------------------------------------------------
    # Docker Operations
    # ------------------------------------------------------------------

    def _ensure_image(self) -> None:
        try:
            self.docker_client.images.get(self.config.image)
            logger.debug("Image '%s' already present locally.", self.config.image)
        except NotFound:
            logger.info("Pulling Kafka image '%s' (this may take a while)...", self.config.image)
            self.docker_client.images.pull(self.config.image)
            logger.info("Image pulled successfully.")

    def _ensure_container(self) -> None:
        try:
            container = self.docker_client.containers.get(self.config.container_name)
            if container.status != "running":
                logger.info("Starting existing container '%s'...", self.config.container_name)
                container.start()
            else:
                logger.debug("Container '%s' is already running.", self.config.container_name)
        except NotFound:
            logger.info("Creating and starting new Kafka container '%s'...", self.config.container_name)
            self.docker_client.containers.run(
                image=self.config.image,
                name=self.config.container_name,
                ports={
                    f"{self.config.internal_port}/tcp": int(
                        self.config.bootstrap_servers.rsplit(":", 1)[1]
                    )
                },
                environment=self.config.environment_vars,
                detach=True,
                remove=True, # Auto-remove on stop
            )

    # ------------------------------------------------------------------
    # Readiness Probes
    # ------------------------------------------------------------------

    def _retry_until_ready(
            self,
            action: Callable[[], bool],
            timeout_msg: str,
            interval: float = 1.0
    ) -> None:
        """Generic polling mechanism with timeout."""
        deadline = time.time() + self.config.readiness_timeout_sec
        while time.time() < deadline:
            if action():
                return
            time.sleep(interval)
        raise KafkaReadinessTimeoutError(f"{timeout_msg} after {self.config.readiness_timeout_sec}s.")

    def _wait_for_readiness(self) -> None:
        """Wait for AdminClient to list topics and Coordinator to be ready."""
        logger.info("Waiting for Kafka API to become responsive at %s...", self.config.bootstrap_servers)

        def _is_api_ready() -> bool:
            try:
                admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
                admin.list_topics(timeout=2)
                return True
            except KafkaException:
                return False

        self._retry_until_ready(_is_api_ready, "Kafka API did not become responsive")

        logger.info("Broker API is up. Verifying group coordinator...")
        self._wait_for_group_coordinator()

    def _wait_for_group_coordinator(self) -> None:
        """Ensures __consumer_offsets is initialized and coordinator is ready."""
        def _is_coordinator_ready() -> bool:
            consumer = None
            try:
                consumer = Consumer({
                    "bootstrap.servers": self.config.bootstrap_servers,
                    "group.id": "__readiness_probe__",
                    "session.timeout.ms": "6000",
                })
                # Attempt to read committed offsets to trigger coordinator interaction
                consumer.committed([_new_topic_partition("__consumer_offsets", 0)], timeout=2)
                return True
            except KafkaException:
                return False
            except Exception as e:
                logger.debug("Unexpected error during coordinator probe: %s", e)
                return False
            finally:
                if consumer is not None:
                    consumer.close()

        self._retry_until_ready(_is_coordinator_ready, "Group coordinator did not become ready")

    # ------------------------------------------------------------------
    # Topic management
    # ------------------------------------------------------------------

    def create_topic(
            self,
            topic_name: str,
            num_partitions: int = 1,
            replication_factor: int = 1,
    ) -> None:
        """Create a Kafka topic idempotently."""
        admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
        new_topic = NewTopic(
            topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )

        futures = admin.create_topics([new_topic], operation_timeout=5)
        for topic, future in futures.items():
            try:
                future.result()
                logger.info("Created topic '%s'.", topic)
            except KafkaException as exc:
                kafka_error = exc.args[0]
                if kafka_error.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                    logger.debug("Topic '%s' already exists; skipping.", topic)
                else:
                    logger.error("Failed to create topic '%s': %s", topic, kafka_error)
                    raise
            except Exception as exc:
                logger.error("Unexpected error creating topic '%s': %s", topic, exc)
                raise

    def create_topics_if_not_exist(
            self, topic_names: List[str], num_partitions: int = 1
    ) -> None:
        """Batch-create topics idempotently."""
        for topic in topic_names:
            self.create_topic(topic, num_partitions=num_partitions)

    def clear_all_topics(self) -> None:
        """Delete every non-internal topic (fast data reset between tests)."""
        admin = AdminClient({"bootstrap.servers": self.config.bootstrap_servers})
        try:
            metadata = admin.list_topics(timeout=5)
            to_delete = [
                t for t in metadata.topics if not t.startswith("__")
            ]
            if not to_delete:
                logger.debug("No user topics to clean up.")
                return

            logger.info("Deleting topics: %s", to_delete)
            futures = admin.delete_topics(to_delete, operation_timeout=5)

            for topic, fut in futures.items():
                try:
                    fut.result()
                    logger.debug("Deleted topic '%s'.", topic)
                except KafkaException as exc:
                    logger.warning("Failed to delete topic '%s': %s", topic, exc.args[0])
        except Exception as exc:
            logger.warning("Topic cleanup process encountered an error: %s", exc)