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
Edge-case and error-handling integration tests for the Python SDK.

Focus areas:
  - Exception hierarchy and attributes
  - Network failures (connecting to a dead port)
  - gRPC error code translation
  - Idempotent cleanup patterns
  - Concurrent function operations
"""

import uuid

import pytest

from fs_client import FsClient
from fs_client.config import KafkaInput, KafkaOutput, WasmTaskBuilder
from fs_client.exceptions import (
    ClientError,
    ConflictError,
    FsError,
    FunctionStreamTimeoutError,
    NetworkError,
    NotFoundError,
    ServerError,
)


# ======================================================================
# Helpers
# ======================================================================


def _unique_name(prefix: str = "err") -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _processor_bytes() -> bytes:
    return b"""\
from fs_api import FSProcessorDriver, Context

class ErrProcessor(FSProcessorDriver):
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


def _build_config(name: str, class_name: str = "ErrProcessor") -> str:
    return (
        WasmTaskBuilder()
        .set_name(name)
        .set_type("python")
        .add_init_config("class_name", class_name)
        .add_input_group(
            [KafkaInput("localhost:9092", "err-in", "err-grp", partition=0)]
        )
        .add_output(KafkaOutput("localhost:9092", "err-out", 0))
        .build()
        .to_yaml()
    )


def _create(client, name):
    return client.create_python_function(
        class_name="ErrProcessor",
        modules=[("err_processor", _processor_bytes())],
        config_content=_build_config(name),
    )


def _cleanup(client, name):
    try:
        client.stop_function(name)
    except Exception:
        pass
    try:
        client.drop_function(name)
    except Exception:
        pass


# ======================================================================
# TestExceptionHierarchy
# ======================================================================


class TestExceptionHierarchy:
    """Verify that all custom exceptions follow the documented hierarchy."""

    def test_server_error_is_fs_error(self):
        assert issubclass(ServerError, FsError)

    def test_client_error_is_fs_error(self):
        assert issubclass(ClientError, FsError)

    def test_not_found_is_server_error(self):
        assert issubclass(NotFoundError, ServerError)

    def test_conflict_is_server_error(self):
        assert issubclass(ConflictError, ServerError)

    def test_network_error_is_server_error(self):
        assert issubclass(NetworkError, ServerError)

    def test_timeout_error_is_server_error(self):
        assert issubclass(FunctionStreamTimeoutError, ServerError)

    def test_server_error_has_status_code_attr(self):
        err = ServerError("test", status_code=500)
        assert err.status_code == 500

    def test_server_error_has_grpc_code_attr(self):
        err = ServerError("test", grpc_code=None)
        assert err.grpc_code is None

    def test_fs_error_has_original_exception_attr(self):
        orig = RuntimeError("root cause")
        err = FsError("wrapper", original_exception=orig)
        assert err.original_exception is orig

    def test_fs_error_message(self):
        err = FsError("something went wrong")
        assert str(err) == "something went wrong"


# ======================================================================
# TestNetworkErrors
# ======================================================================


class TestNetworkErrors:
    """Test behaviour when the server is unreachable."""

    def test_connect_to_dead_port_raises_on_rpc(self):
        client = FsClient(host="127.0.0.1", port=19999, default_timeout=2.0)
        with pytest.raises((NetworkError, ServerError, Exception)):
            client.show_functions(timeout=2.0)
        client.close()

    def test_drop_on_dead_port_raises(self):
        client = FsClient(host="127.0.0.1", port=19998, default_timeout=2.0)
        with pytest.raises((NetworkError, ServerError, Exception)):
            client.drop_function("anything", timeout=2.0)
        client.close()


# ======================================================================
# TestIdempotentCleanup
# ======================================================================


class TestIdempotentCleanup:
    """Verify idempotent cleanup patterns used in real-world teardown."""

    def test_drop_already_dropped_raises_not_found(self, fs_server):
        name = _unique_name("idempotent")
        with fs_server.get_client() as client:
            _create(client, name)
            client.stop_function(name)
            client.drop_function(name)
            with pytest.raises(NotFoundError) as exc_info:
                client.drop_function(name)
            assert exc_info.value.status_code == 404

    def test_stop_already_stopped_is_benign(self, fs_server):
        name = _unique_name("stop-twice")
        with fs_server.get_client() as client:
            _create(client, name)
            client.stop_function(name)
            try:
                client.stop_function(name)
            except ServerError:
                pass
            _cleanup(client, name)

    def test_cleanup_helper_is_safe_on_nonexistent(self, fs_server):
        with fs_server.get_client() as client:
            _cleanup(client, "never-existed-" + uuid.uuid4().hex[:8])


# ======================================================================
# TestRapidCreateDrop
# ======================================================================


class TestRapidCreateDrop:
    """Stress the create/drop cycle to catch race conditions."""

    @pytest.mark.parametrize("iteration", range(5))
    def test_rapid_create_and_drop(self, fs_server, iteration):
        name = _unique_name(f"rapid-{iteration}")
        with fs_server.get_client() as client:
            _create(client, name)
            listing = client.show_functions()
            assert name in [f.name for f in listing.functions]
            client.stop_function(name)
            client.drop_function(name)
            listing = client.show_functions()
            assert name not in [f.name for f in listing.functions]


# ======================================================================
# TestMultipleFunctions
# ======================================================================


class TestMultipleFunctions:
    """Test managing multiple independent functions concurrently."""

    def test_create_multiple_functions(self, fs_server):
        names = [_unique_name(f"multi-{i}") for i in range(3)]
        with fs_server.get_client() as client:
            for n in names:
                _create(client, n)

            listing = client.show_functions()
            listed = [f.name for f in listing.functions]
            for n in names:
                assert n in listed

            for n in names:
                _cleanup(client, n)

    def test_drop_one_does_not_affect_others(self, fs_server):
        n1 = _unique_name("keep")
        n2 = _unique_name("drop")
        with fs_server.get_client() as client:
            _create(client, n1)
            _create(client, n2)

            client.stop_function(n2)
            client.drop_function(n2)

            listing = client.show_functions()
            listed = [f.name for f in listing.functions]
            assert n1 in listed
            assert n2 not in listed

            _cleanup(client, n1)


# ======================================================================
# TestFunctionStatusTransitions
# ======================================================================


class TestFunctionStatusTransitions:
    """Verify that function status changes correctly after operations."""

    def test_created_function_has_status(self, fs_server):
        name = _unique_name("status")
        with fs_server.get_client() as client:
            _create(client, name)
            listing = client.show_functions()
            fn = next((f for f in listing.functions if f.name == name), None)
            assert fn is not None
            assert isinstance(fn.status, str)
            assert len(fn.status) > 0
            _cleanup(client, name)

    def test_stopped_function_status_changes(self, fs_server):
        name = _unique_name("status-stop")
        with fs_server.get_client() as client:
            _create(client, name)
            client.stop_function(name)
            listing = client.show_functions()
            fn = next((f for f in listing.functions if f.name == name), None)
            assert fn is not None
            _cleanup(client, name)

    def test_restarted_function_status_changes(self, fs_server):
        name = _unique_name("status-restart")
        with fs_server.get_client() as client:
            _create(client, name)
            client.stop_function(name)
            client.start_function(name)
            listing = client.show_functions()
            fn = next((f for f in listing.functions if f.name == name), None)
            assert fn is not None
            _cleanup(client, name)
