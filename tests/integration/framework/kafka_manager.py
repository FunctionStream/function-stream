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

Usage::

    mgr = KafkaDockerManager()
    mgr.setup_kafka()
    mgr.create_topics_if_not_exist(["input-topic", "output-topic"])
    ...
    mgr.clear_all_topics()
    mgr.teardown_kafka()
"""

import logging
import time
from typing import List

import docker
from docker.errors import APIError, NotFound
from confluent_kafka.admin import AdminClient, NewTopic

logger = logging.getLogger(__name__)

_DEFAULT_IMAGE = "apache/kafka:3.7.0"
_DEFAULT_CONTAINER = "fs-integration-kafka-broker"
_DEFAULT_BOOTSTRAP = "127.0.0.1:9092"


class KafkaDockerManager:
    """
    Manages a single-node Kafka broker inside a Docker container (KRaft mode).

    The class is intentionally stateless with respect to topics: every public
    method is idempotent so that tests can call ``setup_kafka()`` multiple
    times without side-effects.
    """

    def __init__(
        self,
        image: str = _DEFAULT_IMAGE,
        container_name: str = _DEFAULT_CONTAINER,
        bootstrap_servers: str = _DEFAULT_BOOTSTRAP,
    ) -> None:
        self.docker_client = docker.from_env()
        self.image_name = image
        self.container_name = container_name
        self.bootstrap_servers = bootstrap_servers

    # ------------------------------------------------------------------
    # Full setup / teardown
    # ------------------------------------------------------------------

    def setup_kafka(self) -> None:
        """Pull image -> start container -> wait for readiness."""
        self._ensure_image()
        self._ensure_container()
        self._wait_for_readiness()

    def teardown_kafka(self) -> None:
        """Stop and remove the Kafka container."""
        try:
            container = self.docker_client.containers.get(self.container_name)
            logger.info("Stopping Kafka container '%s' ...", self.container_name)
            container.stop()
        except NotFound:
            pass
        except APIError as exc:
            logger.warning("Error while stopping Kafka: %s", exc)

    # ------------------------------------------------------------------
    # Image management
    # ------------------------------------------------------------------

    def _ensure_image(self) -> None:
        try:
            self.docker_client.images.get(self.image_name)
            logger.info("Image '%s' already present locally.", self.image_name)
        except NotFound:
            logger.info("Pulling Kafka image '%s' ...", self.image_name)
            self.docker_client.images.pull(self.image_name)
            logger.info("Image pulled successfully.")

    # ------------------------------------------------------------------
    # Container management (KRaft single-node, apache/kafka official image)
    # ------------------------------------------------------------------

    def _ensure_container(self) -> None:
        try:
            container = self.docker_client.containers.get(self.container_name)
            if container.status != "running":
                logger.info(
                    "Container '%s' exists but is not running; starting ...",
                    self.container_name,
                )
                container.start()
            else:
                logger.info(
                    "Container '%s' is already running.", self.container_name
                )
        except NotFound:
            logger.info(
                "Creating Kafka container '%s' ...", self.container_name
            )
            env = {
                "KAFKA_NODE_ID": "1",
                "KAFKA_PROCESS_ROLES": "broker,controller",
                "KAFKA_CONTROLLER_LISTENER_NAMES": "CONTROLLER",
                "KAFKA_LISTENERS": (
                    "PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093"
                ),
                "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP": (
                    "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT"
                ),
                "KAFKA_ADVERTISED_LISTENERS": (
                    f"PLAINTEXT://{self.bootstrap_servers}"
                ),
                "KAFKA_CONTROLLER_QUORUM_VOTERS": "1@localhost:9093",
                "KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR": "1",
                "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR": "1",
                "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR": "1",
                "CLUSTER_ID": "fs-integration-test-cluster-01",
            }
            self.docker_client.containers.run(
                image=self.image_name,
                name=self.container_name,
                ports={"9092/tcp": 9092},
                environment=env,
                detach=True,
                remove=True,
            )

    def _wait_for_readiness(self, timeout: int = 60) -> None:
        logger.info(
            "Waiting for Kafka to become ready at %s ...",
            self.bootstrap_servers,
        )
        deadline = time.time() + timeout
        while time.time() < deadline:
            try:
                admin = AdminClient(
                    {"bootstrap.servers": self.bootstrap_servers}
                )
                admin.list_topics(timeout=2)
                logger.info("Kafka is ready.")
                return
            except Exception:
                time.sleep(1)
        raise TimeoutError(
            f"Kafka did not become ready within {timeout}s. "
            "Check Docker logs for details."
        )

    # ------------------------------------------------------------------
    # Topic management
    # ------------------------------------------------------------------

    def create_topic(
        self,
        topic_name: str,
        num_partitions: int = 1,
        replication_factor: int = 1,
    ) -> None:
        """
        Create a Kafka topic idempotently.

        If the topic already exists the call succeeds silently.
        """
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
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
            except Exception as exc:
                if "TOPIC_ALREADY_EXISTS" in str(exc):
                    logger.debug("Topic '%s' already exists; skipping.", topic)
                else:
                    raise

    def create_topics_if_not_exist(
        self, topic_names: List[str], num_partitions: int = 1
    ) -> None:
        """Batch-create topics idempotently."""
        for topic in topic_names:
            self.create_topic(topic, num_partitions=num_partitions)

    def clear_all_topics(self) -> None:
        """Delete every non-internal topic (fast data reset between tests)."""
        admin = AdminClient({"bootstrap.servers": self.bootstrap_servers})
        try:
            metadata = admin.list_topics(timeout=5)
            to_delete = [
                t for t in metadata.topics if not t.startswith("__")
            ]
            if to_delete:
                logger.debug("Deleting leftover topics: %s", to_delete)
                futures = admin.delete_topics(to_delete, operation_timeout=5)
                for _topic, fut in futures.items():
                    fut.result()
        except Exception as exc:
            logger.warning("Topic cleanup failed: %s", exc)
