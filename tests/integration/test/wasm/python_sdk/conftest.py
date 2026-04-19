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

import logging
import re
import sys
from pathlib import Path
from typing import Generator, List

_CURRENT_DIR = Path(__file__).resolve().parent
_INTEGRATION_ROOT = Path(__file__).resolve().parents[3]

if str(_INTEGRATION_ROOT) not in sys.path:
    sys.path.insert(0, str(_INTEGRATION_ROOT))
if str(_CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(_CURRENT_DIR))

import pytest
from framework import FunctionStreamInstance, KafkaDockerManager
from fs_client.client import FsClient

logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def kafka() -> Generator[KafkaDockerManager, None, None]:
    """
    Session-scoped Kafka broker manager.
    Leverages Context Manager for guaranteed teardown.
    """
    with KafkaDockerManager() as mgr:
        yield mgr
        try:
            mgr.clear_all_topics()
        except Exception as e:
            logger.warning("Failed to clear topics during Kafka teardown: %s", e)


@pytest.fixture(scope="session")
def kafka_topics(kafka: KafkaDockerManager) -> str:
    """
    Pre-creates standard topics and returns the bootstrap server address.
    """
    kafka.create_topics_if_not_exist(["in", "out", "events", "counts"])
    return kafka.config.bootstrap_servers


def _sanitize_segment(segment: str) -> str:
    clean = re.sub(r"[^\w\-]+", "_", segment).strip("_")
    return clean or "unknown"


def _nodeid_to_workspace_path(nodeid: str) -> str:
    """
    Convert pytest nodeid into a readable nested path under target/.

    Example:
        test/wasm/python_sdk/test_data_flow.py::TestDataFlow::test_single_word_counting
    ->
        test/wasm/python_sdk/test_data_flow/TestDataFlow/test_single_word_counting
    """
    parts = nodeid.split("::")
    file_part = Path(parts[0]).with_suffix("")
    file_segments = [_sanitize_segment(seg) for seg in file_part.parts]
    extra_segments = [_sanitize_segment(seg) for seg in parts[1:]]
    return str(Path(*file_segments, *extra_segments))


@pytest.fixture
def fs_server(request: pytest.FixtureRequest) -> Generator[FunctionStreamInstance, None, None]:
    """
    Function-scoped FunctionStream instance.
    Uses Context Manager to ensure SIGKILL and workspace cleanup.
    """
    test_name = _nodeid_to_workspace_path(request.node.nodeid)
    with FunctionStreamInstance(test_name=test_name) as instance:
        yield instance


@pytest.fixture
def fs_client(fs_server: FunctionStreamInstance) -> Generator[FsClient, None, None]:
    """
    Function-scoped FsClient connected to the isolated fs_server.
    """
    with fs_server.get_client() as client:
        yield client


@pytest.fixture
def function_registry(fs_client: FsClient) -> Generator[List[str], None, None]:
    """
    RAII-style registry for FunctionStream tasks.
    Ensures absolute teardown of functions to prevent state leakage.
    """
    registered_names: List[str] = []

    yield registered_names

    for name in registered_names:
        try:
            fs_client.stop_function(name)
        except Exception as e:
            logger.debug("Failed to stop function '%s' during cleanup: %s", name, e)

        try:
            fs_client.drop_function(name)
        except Exception as e:
            logger.error("Failed to drop function '%s' during cleanup: %s", name, e)