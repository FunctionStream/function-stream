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
Global pytest fixtures for the FunctionStream Python SDK integration tests.
Provides managed Kafka broker, server instances, client connections,
and automated resource cleanup.
"""

import sys
from pathlib import Path
from typing import Generator, List

import pytest

# tests/integration/test/wasm/python_sdk -> parents[3] = tests/integration
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))
# Make the ``processors`` package importable from this directory
sys.path.insert(0, str(Path(__file__).resolve().parent))

from framework import FunctionStreamInstance, KafkaDockerManager  # noqa: E402
from fs_client.client import FsClient  # noqa: E402

PROJECT_ROOT = Path(__file__).resolve().parents[5]


# ======================================================================
# Kafka broker (opt-in, independent of fs_server)
# ======================================================================

@pytest.fixture(scope="session")
def kafka() -> Generator[KafkaDockerManager, None, None]:
    """
    Session-scoped fixture: start a Docker-managed Kafka broker once
    for the entire test session.

    Tests that need a live Kafka broker should declare this fixture
    as a parameter.  Tests that only register functions (without
    producing / consuming data) do NOT need this fixture.
    """
    mgr = KafkaDockerManager()
    mgr.setup_kafka()
    yield mgr
    mgr.clear_all_topics()
    mgr.teardown_kafka()


# ======================================================================
# FunctionStream server
# ======================================================================

@pytest.fixture(scope="session")
def fs_server() -> Generator[FunctionStreamInstance, None, None]:
    """
    Session-scoped fixture: start the FunctionStream server once for all tests.
    """
    instance = FunctionStreamInstance(test_name="wasm_python_sdk_integration")
    instance.start()
    yield instance
    instance.kill()


# ======================================================================
# Client & resource tracking
# ======================================================================

@pytest.fixture
def fs_client(fs_server: FunctionStreamInstance) -> Generator[FsClient, None, None]:
    """
    Function-scoped fixture: provide a fresh client connected to the server.
    The connection is automatically closed after each test.
    """
    with fs_server.get_client() as client:
        yield client


@pytest.fixture
def function_registry(fs_client: FsClient) -> Generator[List[str], None, None]:
    """
    RAII Resource Manager: tracks registered function names.
    Automatically stops and drops every tracked function after each test,
    guaranteeing environment idempotency regardless of assertion failures.
    """
    registered_names: List[str] = []

    yield registered_names

    for name in registered_names:
        try:
            fs_client.stop_function(name)
        except Exception:
            pass
        try:
            fs_client.drop_function(name)
        except Exception:
            pass
