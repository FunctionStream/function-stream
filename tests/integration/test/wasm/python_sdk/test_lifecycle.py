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

import uuid
from typing import Any, List, Optional

from fs_client.client import FsClient
from fs_client.config import KafkaInput, KafkaOutput, WasmTaskBuilder
from processors.counter_processor import CounterProcessor

EXPECTED_STOPPED_STATES = frozenset(["STOPPED", "PAUSED", "INITIALIZED"])
EXPECTED_RUNNING_STATES = frozenset(["RUNNING"])


def _generate_unique_name(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _create_counter_task_builder(fn_name: str, bootstrap: str) -> WasmTaskBuilder:
    return (
        WasmTaskBuilder()
        .set_name(fn_name)
        .add_init_config("class_name", "CounterProcessor")
        .add_input_group([KafkaInput(bootstrap, "in", "grp", 0)])
        .add_output(KafkaOutput(bootstrap, "out", 0))
    )


def _get_function_info(fs_client: FsClient, fn_name: str) -> Optional[Any]:
    listing = fs_client.show_functions()
    for fn in listing.functions:
        if fn.name == fn_name:
            return fn
    return None


class TestFunctionLifecycle:

    def test_full_lifecycle_transitions(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        fn_name = _generate_unique_name("lifecycle")
        function_registry.append(fn_name)

        config = _create_counter_task_builder(fn_name, kafka_topics).add_init_config("test_mode", "true").build()

        assert fs_client.create_python_function_from_config(config, CounterProcessor) is True

        fn_info = _get_function_info(fs_client, fn_name)
        assert fn_info is not None
        assert bool(fn_info.status)

        assert fs_client.stop_function(fn_name) is True

        assert fs_client.start_function(fn_name) is True
        fn_info = _get_function_info(fs_client, fn_name)
        assert fn_info is not None
        assert fn_info.status.upper() in EXPECTED_RUNNING_STATES

        assert fs_client.stop_function(fn_name) is True
        fn_info = _get_function_info(fs_client, fn_name)
        assert fn_info is not None
        assert fn_info.status.upper() in EXPECTED_STOPPED_STATES

        assert fs_client.drop_function(fn_name) is True
        assert _get_function_info(fs_client, fn_name) is None

        if fn_name in function_registry:
            function_registry.remove(fn_name)

    def test_show_functions_returns_created_function(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        fn_name = _generate_unique_name("show")
        function_registry.append(fn_name)

        config = _create_counter_task_builder(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        assert _get_function_info(fs_client, fn_name) is not None

    def test_show_functions_result_fields(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        fn_name = _generate_unique_name("fields")
        function_registry.append(fn_name)

        config = _create_counter_task_builder(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        fn_info = _get_function_info(fs_client, fn_name)
        assert fn_info is not None
        assert fn_info.name == fn_name
        assert bool(fn_info.task_type)
        assert bool(fn_info.status)

    def test_multiple_functions_coexist(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        names = [_generate_unique_name("multi") for _ in range(3)]
        function_registry.extend(names)

        for name in names:
            config = _create_counter_task_builder(name, kafka_topics).build()
            fs_client.create_python_function_from_config(config, CounterProcessor)

        listing = fs_client.show_functions()
        listed_names = {f.name for f in listing.functions}

        for name in names:
            assert name in listed_names

    def test_rapid_create_drop_cycle(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        for i in range(5):
            fn_name = _generate_unique_name(f"rapid-{i}")
            config = _create_counter_task_builder(fn_name, kafka_topics).build()

            fs_client.create_python_function_from_config(config, CounterProcessor)
            fs_client.stop_function(fn_name)
            assert fs_client.drop_function(fn_name) is True

        listing = fs_client.show_functions()
        remaining = [f.name for f in listing.functions if f.name.startswith("rapid-")]
        assert not remaining

    def test_restart_preserves_identity(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        fn_name = _generate_unique_name("restart")
        function_registry.append(fn_name)

        config = _create_counter_task_builder(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        fs_client.stop_function(fn_name)
        fs_client.start_function(fn_name)

        fn_info = _get_function_info(fs_client, fn_name)
        assert fn_info is not None
        assert fn_info.name == fn_name
        assert fn_info.status.upper() in EXPECTED_RUNNING_STATES

    def test_stop_then_drop(
            self, fs_client: FsClient, function_registry: List[str], kafka_topics: str
    ) -> None:
        fn_name = _generate_unique_name("stop-drop")
        function_registry.append(fn_name)

        config = _create_counter_task_builder(fn_name, kafka_topics).build()
        fs_client.create_python_function_from_config(config, CounterProcessor)

        assert fs_client.stop_function(fn_name) is True
        assert fs_client.drop_function(fn_name) is True

        assert _get_function_info(fs_client, fn_name) is None

        if fn_name in function_registry:
            function_registry.remove(fn_name)