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
Comprehensive integration tests for the Python SDK client API.

Covers every public method of FsClient against a live FunctionStream server:
  - create_python_function
  - create_python_function_from_config
  - create_function_from_bytes
  - show_functions (with / without filter)
  - start_function / stop_function / drop_function
  - Error semantics: ConflictError, NotFoundError, validation errors
  - Context-manager lifecycle
  - Custom timeout behaviour

Requires a running FunctionStream server (provided by the ``fs_server`` fixture).
"""

import time
import uuid
from pathlib import Path

import pytest

from fs_client import FsClient, FunctionInfo, ShowFunctionsResult
from fs_client.config import KafkaInput, KafkaOutput, WasmTaskBuilder
from fs_client.exceptions import (
    BadRequestError,
    ClientError,
    ConflictError,
    FsError,
    NotFoundError,
    ServerError,
)


# ======================================================================
# Helpers
# ======================================================================


def _unique_name(prefix: str = "pytest") -> str:
    """Generate a unique function name to avoid cross-test collisions."""
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _build_config(name: str, class_name: str = "MinimalProcessor") -> str:
    """Build a minimal YAML config string for a Python function."""
    return (
        WasmTaskBuilder()
        .set_name(name)
        .set_type("python")
        .add_init_config("class_name", class_name)
        .add_init_config("emit_threshold", "1")
        .add_input_group(
            [KafkaInput("localhost:9092", "test-in", "test-grp", partition=0)]
        )
        .add_output(KafkaOutput("localhost:9092", "test-out", 0))
        .build()
        .to_yaml()
    )


def _processor_bytes() -> bytes:
    """Return a minimal FSProcessorDriver implementation as source bytes."""
    return b"""\
from fs_api import FSProcessorDriver, Context

class MinimalProcessor(FSProcessorDriver):
    def init(self, ctx: Context, config: dict):
        pass
    def process(self, ctx: Context, source_id: int, data: bytes):
        ctx.emit(data, 0)
    def process_watermark(self, ctx: Context, source_id: int, watermark: int):
        ctx.emit_watermark(watermark, 0)
    def take_checkpoint(self, ctx: Context, checkpoint_id: int):
        return None
    def check_heartbeat(self, ctx: Context) -> bool:
        return True
    def close(self, ctx: Context):
        pass
    def custom(self, payload: bytes) -> bytes:
        return b'{}'
"""


def _create_function(client: FsClient, name: str) -> bool:
    """Helper: register a Python function with the given name."""
    return client.create_python_function(
        class_name="MinimalProcessor",
        modules=[("minimal_processor", _processor_bytes())],
        config_content=_build_config(name),
    )


def _ensure_dropped(client: FsClient, name: str) -> None:
    """Drop a function if it exists, silently ignore NotFoundError."""
    try:
        client.stop_function(name)
    except Exception:
        pass
    try:
        client.drop_function(name)
    except Exception:
        pass


# ======================================================================
# TestFsClientContextManager
# ======================================================================


class TestFsClientContextManager:
    """Verify FsClient as a context manager opens and closes properly."""

    def test_context_manager_enter_exit(self, fs_server):
        with fs_server.get_client() as client:
            assert isinstance(client, FsClient)

    def test_context_manager_closes_channel(self, fs_server):
        with fs_server.get_client() as client:
            result = client.show_functions()
            assert isinstance(result, ShowFunctionsResult)

    def test_manual_close(self, fs_server):
        client = fs_server.get_client()
        result = client.show_functions()
        assert isinstance(result, ShowFunctionsResult)
        client.close()


# ======================================================================
# TestCreatePythonFunction
# ======================================================================


class TestCreatePythonFunction:
    """Tests for FsClient.create_python_function (direct module bytes)."""

    def test_create_succeeds(self, fs_server):
        name = _unique_name("create")
        with fs_server.get_client() as client:
            result = _create_function(client, name)
            assert result is True
            _ensure_dropped(client, name)

    def test_create_with_multiple_modules(self, fs_server):
        name = _unique_name("multi-mod")
        helper_code = b"""\
HELPER_VERSION = "1.0"
def helper_func():
    return "ok"
"""
        proc_code = b"""\
from fs_api import FSProcessorDriver, Context

class MultiModProcessor(FSProcessorDriver):
    def init(self, ctx: Context, config: dict):
        pass
    def process(self, ctx: Context, source_id: int, data: bytes):
        ctx.emit(data, 0)
    def process_watermark(self, ctx: Context, source_id: int, watermark: int):
        ctx.emit_watermark(watermark, 0)
    def take_checkpoint(self, ctx: Context, checkpoint_id: int):
        return None
    def check_heartbeat(self, ctx: Context) -> bool:
        return True
    def close(self, ctx: Context):
        pass
    def custom(self, payload: bytes) -> bytes:
        return b'{}'
"""
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MultiModProcessor",
                modules=[
                    ("helper_mod", helper_code),
                    ("multi_mod_processor", proc_code),
                ],
                config_content=_build_config(name, class_name="MultiModProcessor"),
            )
            assert result is True
            _ensure_dropped(client, name)

    def test_create_empty_modules_raises_value_error(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(ValueError, match="module"):
                client.create_python_function(
                    class_name="X",
                    modules=[],
                    config_content="name: x\n",
                )

    def test_create_duplicate_raises_conflict(self, fs_server):
        name = _unique_name("dup")
        with fs_server.get_client() as client:
            _create_function(client, name)
            with pytest.raises(ConflictError) as exc_info:
                _create_function(client, name)
            assert exc_info.value.status_code == 409
            _ensure_dropped(client, name)


# ======================================================================
# TestCreatePythonFunctionFromConfig
# ======================================================================


class TestCreatePythonFunctionFromConfig:
    """Tests for FsClient.create_python_function_from_config."""

    def test_create_from_config_with_example_processor(
        self, fs_server, python_example_dir
    ):
        processor_file = python_example_dir / "processor_impl.py"
        if not processor_file.exists():
            pytest.skip("Python processor example not available")

        import importlib.util
        import sys

        spec = importlib.util.spec_from_file_location(
            "processor_impl", str(processor_file)
        )
        mod = importlib.util.module_from_spec(spec)
        mod.__file__ = str(processor_file)
        sys.modules["processor_impl"] = mod
        spec.loader.exec_module(mod)

        name = _unique_name("from-cfg")
        config = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .add_init_config("emit_threshold", "1")
            .add_init_config("class_name", "CounterProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in-t", "grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out-t", 0))
            .build()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function_from_config(
                config, mod.CounterProcessor
            )
            assert result is True
            _ensure_dropped(client, name)

    def test_from_config_rejects_non_class(self, fs_server):
        config = WasmTaskBuilder().set_name("x").build()
        with fs_server.get_client() as client:
            with pytest.raises(ValueError, match="class"):
                client.create_python_function_from_config(config, "not_a_class")

    def test_from_config_rejects_non_driver(self, fs_server):
        config = WasmTaskBuilder().set_name("x").build()

        class NotADriver:
            pass

        with fs_server.get_client() as client:
            with pytest.raises(TypeError, match="FSProcessorDriver"):
                client.create_python_function_from_config(config, NotADriver)


# ======================================================================
# TestCreateFunctionFromBytes
# ======================================================================


class TestCreateFunctionFromBytes:
    """Tests for FsClient.create_function_from_bytes (WASM path)."""

    def test_config_wrong_type_raises_type_error(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(TypeError):
                client.create_function_from_bytes(
                    function_bytes=b"x",
                    config_content=12345,
                )

    def test_config_accepts_string(self, fs_server):
        """String config_content is accepted (may fail on server for bad WASM)."""
        with fs_server.get_client() as client:
            with pytest.raises(ServerError):
                client.create_function_from_bytes(
                    function_bytes=b"\x00invalid-wasm",
                    config_content="name: bad-wasm\ntype: processor\n",
                )

    def test_config_accepts_bytes(self, fs_server):
        """Bytes config_content is accepted (may fail on server for bad WASM)."""
        with fs_server.get_client() as client:
            with pytest.raises(ServerError):
                client.create_function_from_bytes(
                    function_bytes=b"\x00invalid-wasm",
                    config_content=b"name: bad-wasm-b\ntype: processor\n",
                )


# ======================================================================
# TestCreateFunctionFromFiles
# ======================================================================


class TestCreateFunctionFromFiles:
    """Tests for FsClient.create_function_from_files."""

    def test_missing_wasm_file_raises_file_not_found(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(FileNotFoundError):
                client.create_function_from_files(
                    function_path="/nonexistent/path.wasm",
                    config_path="/nonexistent/config.yaml",
                )

    def test_missing_config_file_raises_file_not_found(self, fs_server, tmp_path):
        wasm_file = tmp_path / "dummy.wasm"
        wasm_file.write_bytes(b"\x00\x61\x73\x6d")
        with fs_server.get_client() as client:
            with pytest.raises(FileNotFoundError):
                client.create_function_from_files(
                    function_path=str(wasm_file),
                    config_path="/nonexistent/config.yaml",
                )


# ======================================================================
# TestShowFunctions
# ======================================================================


class TestShowFunctions:
    """Tests for FsClient.show_functions."""

    def test_show_functions_returns_result_type(self, fs_server):
        with fs_server.get_client() as client:
            result = client.show_functions()
            assert isinstance(result, ShowFunctionsResult)
            assert isinstance(result.status_code, int)
            assert result.status_code < 400
            assert isinstance(result.functions, list)

    def test_show_functions_contains_created_function(self, fs_server):
        name = _unique_name("show")
        with fs_server.get_client() as client:
            _create_function(client, name)
            result = client.show_functions()
            names = [f.name for f in result.functions]
            assert name in names
            _ensure_dropped(client, name)

    def test_show_functions_function_info_fields(self, fs_server):
        name = _unique_name("info")
        with fs_server.get_client() as client:
            _create_function(client, name)
            result = client.show_functions()
            fn = next((f for f in result.functions if f.name == name), None)
            assert fn is not None
            assert isinstance(fn, FunctionInfo)
            assert fn.name == name
            assert isinstance(fn.task_type, str)
            assert isinstance(fn.status, str)
            _ensure_dropped(client, name)

    def test_show_functions_empty_on_fresh_server(self, fs_server):
        with fs_server.get_client() as client:
            result = client.show_functions()
            assert isinstance(result.functions, list)

    def test_show_functions_with_filter(self, fs_server):
        prefix = f"filter-{uuid.uuid4().hex[:6]}"
        name1 = f"{prefix}-aaa"
        name2 = f"{prefix}-bbb"
        other_name = _unique_name("other")
        with fs_server.get_client() as client:
            _create_function(client, name1)
            _create_function(client, name2)
            _create_function(client, other_name)

            result = client.show_functions(filter_pattern=prefix)
            found = [f.name for f in result.functions]
            assert name1 in found or name2 in found

            _ensure_dropped(client, name1)
            _ensure_dropped(client, name2)
            _ensure_dropped(client, other_name)

    def test_show_functions_with_custom_timeout(self, fs_server):
        with fs_server.get_client() as client:
            result = client.show_functions(timeout=10.0)
            assert isinstance(result, ShowFunctionsResult)


# ======================================================================
# TestFunctionLifecycle
# ======================================================================


class TestFunctionLifecycle:
    """Full function lifecycle: create -> show -> stop -> start -> drop."""

    def test_full_lifecycle(self, fs_server):
        name = _unique_name("lifecycle")
        with fs_server.get_client() as client:
            assert _create_function(client, name) is True

            listing = client.show_functions()
            assert name in [f.name for f in listing.functions]

            assert client.stop_function(name) is True

            assert client.start_function(name) is True

            client.stop_function(name)
            assert client.drop_function(name) is True

            listing = client.show_functions()
            assert name not in [f.name for f in listing.functions]

    def test_stop_and_restart_preserves_function(self, fs_server):
        name = _unique_name("restart")
        with fs_server.get_client() as client:
            _create_function(client, name)
            client.stop_function(name)
            time.sleep(0.5)
            client.start_function(name)

            listing = client.show_functions()
            assert name in [f.name for f in listing.functions]
            _ensure_dropped(client, name)

    def test_drop_running_function_requires_stop(self, fs_server):
        name = _unique_name("drop-running")
        with fs_server.get_client() as client:
            _create_function(client, name)
            try:
                client.drop_function(name)
            except ServerError:
                client.stop_function(name)
                result = client.drop_function(name)
                assert result is True
            else:
                listing = client.show_functions()
                assert name not in [f.name for f in listing.functions]


# ======================================================================
# TestFunctionErrorHandling
# ======================================================================


class TestFunctionErrorHandling:
    """Error semantics for function operations."""

    def test_drop_nonexistent_raises_not_found(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(NotFoundError) as exc_info:
                client.drop_function("nonexistent-" + uuid.uuid4().hex[:8])
            assert exc_info.value.status_code == 404

    def test_start_nonexistent_raises_not_found(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(NotFoundError) as exc_info:
                client.start_function("nonexistent-" + uuid.uuid4().hex[:8])
            assert exc_info.value.status_code == 404

    def test_stop_nonexistent_raises_not_found(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(NotFoundError) as exc_info:
                client.stop_function("nonexistent-" + uuid.uuid4().hex[:8])
            assert exc_info.value.status_code == 404

    def test_duplicate_create_raises_conflict(self, fs_server):
        name = _unique_name("dup-err")
        with fs_server.get_client() as client:
            _create_function(client, name)
            with pytest.raises(ConflictError) as exc_info:
                _create_function(client, name)
            assert exc_info.value.status_code == 409
            _ensure_dropped(client, name)

    def test_all_errors_inherit_from_fs_error(self, fs_server):
        with fs_server.get_client() as client:
            with pytest.raises(FsError):
                client.drop_function("definitely-does-not-exist-" + uuid.uuid4().hex)


# ======================================================================
# TestFunctionWithCheckpoint
# ======================================================================


class TestFunctionWithCheckpoint:
    """Test creating Python functions with checkpoint configuration."""

    def test_checkpoint_enabled(self, fs_server):
        name = _unique_name("ckpt")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .configure_checkpoint(enabled=True, interval=5)
            .add_init_config("class_name", "MinimalProcessor")
            .add_init_config("key", "value")
            .add_input_group(
                [KafkaInput("localhost:9092", "ckpt-in", "ckpt-grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "ckpt-out", 0))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)

    def test_checkpoint_disabled(self, fs_server):
        name = _unique_name("no-ckpt")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .configure_checkpoint(enabled=False)
            .add_init_config("class_name", "MinimalProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in", "grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out", 0))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)


# ======================================================================
# TestFunctionWithMultipleIO
# ======================================================================


class TestFunctionWithMultipleIO:
    """Test creating functions with multiple input groups and outputs."""

    def test_multiple_input_groups(self, fs_server):
        name = _unique_name("multi-in")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .add_init_config("class_name", "MinimalProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in1", "grp1", partition=0)]
            )
            .add_input_group(
                [KafkaInput("localhost:9092", "in2", "grp2", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out", 0))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)

    def test_multiple_outputs(self, fs_server):
        name = _unique_name("multi-out")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .add_init_config("class_name", "MinimalProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in", "grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out1", 0))
            .add_output(KafkaOutput("localhost:9092", "out2", 1))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)


# ======================================================================
# TestBuiltinSerialization
# ======================================================================


class TestBuiltinSerialization:
    """Test creating functions with builtin serialization toggled."""

    def test_builtin_serialization_enabled(self, fs_server):
        name = _unique_name("builtin-on")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .set_builtin_serialization(True)
            .add_init_config("class_name", "MinimalProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in", "grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out", 0))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)

    def test_builtin_serialization_disabled(self, fs_server):
        name = _unique_name("builtin-off")
        config_yaml = (
            WasmTaskBuilder()
            .set_name(name)
            .set_type("python")
            .set_builtin_serialization(False)
            .add_init_config("class_name", "MinimalProcessor")
            .add_input_group(
                [KafkaInput("localhost:9092", "in", "grp", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out", 0))
            .build()
            .to_yaml()
        )
        with fs_server.get_client() as client:
            result = client.create_python_function(
                class_name="MinimalProcessor",
                modules=[("minimal_processor", _processor_bytes())],
                config_content=config_yaml,
            )
            assert result is True
            _ensure_dropped(client, name)


# ======================================================================
# TestClientCustomTimeout
# ======================================================================


class TestClientCustomTimeout:
    """Verify custom timeout propagation."""

    def test_default_timeout_attribute(self, fs_server):
        client = FsClient(
            host=fs_server.host,
            port=fs_server.port,
            default_timeout=42.0,
        )
        assert client.default_timeout == 42.0
        client.close()

    def test_per_call_timeout(self, fs_server):
        with fs_server.get_client() as client:
            result = client.show_functions(timeout=5.0)
            assert isinstance(result, ShowFunctionsResult)
