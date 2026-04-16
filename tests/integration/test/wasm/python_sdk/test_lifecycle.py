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
Integration tests for basic CRUD operations and status state-machine transitions.

Covers:
  - Full lifecycle:  Create -> Show -> Stop -> Start -> Stop -> Drop
  - Multiple function coexistence
  - Rapid create / drop cycling
  - show_functions listing correctness
"""

import uuid
from typing import List

from fs_client.client import FsClient
from fs_client.config import WasmTaskBuilder, KafkaInput, KafkaOutput

from processors.counter_processor import CounterProcessor


def _unique(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _build_counter_config(fn_name: str, bootstrap: str) -> "WasmTaskBuilder":
    """Return a ready-to-build builder pre-configured for CounterProcessor."""
    return (
        WasmTaskBuilder()
        .set_name(fn_name)
        .add_init_config("class_name", "CounterProcessor")
        .add_input_group([KafkaInput(bootstrap, "in", "grp", 0)])
        .add_output(KafkaOutput(bootstrap, "out", 0))
    )


class TestFunctionLifecycle:
    """
    Integration tests for basic CRUD operations and status state machine
    transitions.
    """

    # ------------------------------------------------------------------
    # Golden-path lifecycle
    # ------------------------------------------------------------------

    def test_full_lifecycle_transitions(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Test the golden path: Create -> Show -> Stop -> Start -> Stop -> Drop."""
        fn_name = _unique("lifecycle")
        function_registry.append(fn_name)

        config = _build_counter_config(fn_name, kafka_topics).add_init_config("test_mode", "true").build()

        # 1. Create
        assert fs_client.create_python_function_from_config(config, CounterProcessor) is True

        # 2. Verify visibility
        listing = fs_client.show_functions()
        fn_info = next((f for f in listing.functions if f.name == fn_name), None)
        assert fn_info is not None, "Function must be listed after creation"
        assert fn_info.status, "Status must be a non-empty string"

        # 3. Stop (server may auto-start on creation; stop first to get a known state)
        assert fs_client.stop_function(fn_name) is True

        # 4. Start
        assert fs_client.start_function(fn_name) is True
        listing = fs_client.show_functions()
        fn_info = next(f for f in listing.functions if f.name == fn_name)
        assert fn_info.status.upper() == "RUNNING", (
            f"Expected RUNNING after start, got {fn_info.status}"
        )

        # 5. Stop again
        assert fs_client.stop_function(fn_name) is True
        listing = fs_client.show_functions()
        fn_info = next(f for f in listing.functions if f.name == fn_name)
        assert fn_info.status.upper() in ("STOPPED", "PAUSED", "INITIALIZED"), (
            f"Expected STOPPED/PAUSED after stop, got {fn_info.status}"
        )

        # 6. Drop
        assert fs_client.drop_function(fn_name) is True
        listing = fs_client.show_functions()
        assert fn_name not in [f.name for f in listing.functions], (
            "Function must be removed from registry after drop"
        )
        function_registry.remove(fn_name)

    # ------------------------------------------------------------------
    # show_functions consistency
    # ------------------------------------------------------------------

    def test_show_functions_returns_created_function(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """show_functions must contain the newly created function."""
        fn_name = _unique("show")
        function_registry.append(fn_name)

        config = _build_counter_config(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        listing = fs_client.show_functions()
        names = [f.name for f in listing.functions]
        assert fn_name in names

    def test_show_functions_result_fields(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Each FunctionInfo must carry name, task_type, and status."""
        fn_name = _unique("fields")
        function_registry.append(fn_name)

        config = _build_counter_config(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        listing = fs_client.show_functions()
        fn_info = next(f for f in listing.functions if f.name == fn_name)

        assert fn_info.name == fn_name
        assert fn_info.task_type, "task_type must not be empty"
        assert fn_info.status, "status must not be empty"

    # ------------------------------------------------------------------
    # Multiple function coexistence
    # ------------------------------------------------------------------

    def test_multiple_functions_coexist(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Several independently created functions must all be listed."""
        names = [_unique("multi") for _ in range(3)]
        function_registry.extend(names)

        for name in names:
            config = _build_counter_config(name, kafka_topics).build()
            fs_client.create_python_function_from_config(config, CounterProcessor)

        listing = fs_client.show_functions()
        listed_names = {f.name for f in listing.functions}
        for name in names:
            assert name in listed_names, f"{name} missing from listing"

    # ------------------------------------------------------------------
    # Rapid create / drop cycling
    # ------------------------------------------------------------------

    def test_rapid_create_drop_cycle(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Rapidly creating and dropping functions must not corrupt server state."""
        for i in range(5):
            fn_name = _unique(f"rapid-{i}")

            config = _build_counter_config(fn_name, kafka_topics).build()
            fs_client.create_python_function_from_config(config, CounterProcessor)

            fs_client.stop_function(fn_name)
            assert fs_client.drop_function(fn_name) is True

        listing = fs_client.show_functions()
        remaining = [f.name for f in listing.functions if f.name.startswith("rapid-")]
        assert remaining == [], f"Stale functions remain: {remaining}"

    # ------------------------------------------------------------------
    # Restart (stop + start) resilience
    # ------------------------------------------------------------------

    def test_restart_preserves_identity(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Stopping and restarting a function should keep its name and type."""
        fn_name = _unique("restart")
        function_registry.append(fn_name)

        config = _build_counter_config(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        fs_client.stop_function(fn_name)
        fs_client.start_function(fn_name)

        listing = fs_client.show_functions()
        fn_info = next(f for f in listing.functions if f.name == fn_name)
        assert fn_info.name == fn_name
        assert fn_info.status.upper() == "RUNNING"

    # ------------------------------------------------------------------
    # Drop after stop (explicit two-phase teardown)
    # ------------------------------------------------------------------

    def test_stop_then_drop(
        self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ):
        """Explicitly stopping, then dropping must always succeed."""
        fn_name = _unique("stop-drop")
        function_registry.append(fn_name)

        config = _build_counter_config(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        assert fs_client.stop_function(fn_name) is True
        assert fs_client.drop_function(fn_name) is True

        listing = fs_client.show_functions()
        assert fn_name not in [f.name for f in listing.functions]
        function_registry.remove(fn_name)
