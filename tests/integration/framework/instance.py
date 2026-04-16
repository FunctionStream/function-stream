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
FunctionStreamInstance: the Facade that composes workspace, config,
and process into a single easy-to-use API for test cases.
"""

import logging
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING, Any, Optional, Type

from .config import InstanceConfig, PathManager
from .process import FunctionStreamProcess
from .utils import find_free_port, wait_for_port
from .workspace import InstanceWorkspace

if TYPE_CHECKING:
    from fs_client import FsClient
    from fs_client._proto.function_stream_pb2 import SqlResponse


class FunctionStreamInstanceError(Exception):
    """Base exception for FunctionStream instance errors."""
    pass


class ServerStartupError(FunctionStreamInstanceError):
    """Raised when the server fails to start or bind to the port within the timeout."""
    pass


class MissingDependencyError(FunctionStreamInstanceError):
    """Raised when an optional client dependency (e.g., grpc) is missing."""
    pass


class FunctionStreamInstance:
    """
    Facade for a single FunctionStream server used in integration tests.

    Designed to be used as a context manager to guarantee resource cleanup.

    Usage:
        with FunctionStreamInstance("my_test").configure(**{"service.debug": True}) as inst:
            client = inst.get_client()
            inst.execute_sql("SELECT 1;")
    """

    def __init__(
            self,
            test_name: str = "unnamed",
            host: str = "127.0.0.1",
            binary_path: Optional[Path] = None,
    ):
        self.test_name = test_name
        self.host = host
        self.port = find_free_port()

        self._logger = logging.getLogger(f"{__name__}.{self.test_name}-{self.port}")

        actual_binary = binary_path or PathManager.get_binary_path()
        target_dir = PathManager.get_target_dir()

        self.workspace = InstanceWorkspace(target_dir, test_name, self.port)
        self.config = InstanceConfig(self.host, self.port, self.workspace)
        self.process = FunctionStreamProcess(actual_binary, self.workspace)

    # ------------------------------------------------------------------
    # Context Manager Protocol (Replaces __del__)
    # ------------------------------------------------------------------

    def __enter__(self) -> "FunctionStreamInstance":
        """Start the instance when entering the 'with' block."""
        self.start()
        return self

    def __exit__(
            self,
            exc_type: Optional[Type[BaseException]],
            exc_val: Optional[BaseException],
            exc_tb: Optional[TracebackType],
    ) -> None:
        """Ensure absolute cleanup when exiting the 'with' block."""
        self._logger.debug("Tearing down instance due to context exit.")
        self.kill()

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self.process.is_running

    @property
    def pid(self) -> Optional[int]:
        return self.process.pid

    # ------------------------------------------------------------------
    # Configuration (before start)
    # ------------------------------------------------------------------

    def configure(self, **overrides: Any) -> "FunctionStreamInstance":
        """
        Apply config overrides. Chainable.
        Example: inst.configure(service__debug=True).start()
        """
        self.config.override(overrides)
        return self

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, timeout: float = 30.0) -> "FunctionStreamInstance":
        """Prepare workspace, write config, launch binary, wait for ready."""
        if self.is_running:
            self._logger.debug("Instance is already running. Skipping start.")
            return self

        self.workspace.setup()
        self.config.write_to_workspace()

        self._logger.info(
            "Starting FunctionStream [port=%d, dir=%s]",
            self.port,
            self.workspace.root_dir,
        )
        self.process.start()

        if not wait_for_port(self.host, self.port, timeout):
            stderr_tail = self._read_tail(self.workspace.stderr_file)
            self.process.kill()
            raise ServerStartupError(
                f"Server did not become ready within {timeout}s on port {self.port}.\n"
                f"Process stderr tail:\n{stderr_tail}"
            )

        self._logger.info("FunctionStream ready [pid=%s]", self.pid)
        return self

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful SIGTERM shutdown, then remove everything except logs."""
        if self.is_running:
            self._logger.debug("Stopping instance gracefully...")
            self.process.stop(timeout=timeout)
        self.workspace.cleanup()

    def kill(self) -> None:
        """Immediate SIGKILL, then remove everything except logs."""
        if self.is_running:
            self._logger.debug("Killing instance immediately...")
            self.process.kill()
        self.workspace.cleanup()

    def restart(self, timeout: float = 10.0) -> "FunctionStreamInstance":
        """Stop then start (same port, same workspace)."""
        self._logger.info("Restarting instance...")
        self.stop(timeout=timeout)
        return self.start()

    # ------------------------------------------------------------------
    # Client access
    # ------------------------------------------------------------------

    def get_client(self) -> "FsClient":
        """Return a connected FsClient. Caller should close it when done."""
        try:
            from fs_client import FsClient
        except ImportError as e:
            raise MissingDependencyError(
                "functionstream-client is not installed. "
                "Run: pip install -e python/functionstream-client"
            ) from e
        return FsClient(host=self.host, port=self.port)

    def execute_sql(self, sql: str, timeout: float = 30.0) -> "SqlResponse":
        """Convenience helper: send a SQL statement via gRPC ExecuteSql."""
        try:
            import grpc
            from fs_client._proto import function_stream_pb2, function_stream_pb2_grpc
        except ImportError as e:
            raise MissingDependencyError(
                "gRPC dependencies or proto definitions are not available."
            ) from e

        # Use context manager for channel to prevent socket leaks
        with grpc.insecure_channel(f"{self.host}:{self.port}") as channel:
            stub = function_stream_pb2_grpc.FunctionStreamServiceStub(channel)
            request = function_stream_pb2.SqlRequest(sql=sql)
            self._logger.debug("Executing SQL: %s", sql)
            return stub.ExecuteSql(request, timeout=timeout)

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _read_tail(path: Path, chars: int = 2000) -> str:
        """Reads the end of a file safely, useful for pulling crash logs."""
        if not path.is_file():
            return "<file not found or not a regular file>"
        try:
            text = path.read_text(errors="replace")
            return text[-chars:] if len(text) > chars else text
        except Exception as e:
            return f"<failed to read log: {e}>"

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return f"<FunctionStreamInstance test='{self.test_name}' port={self.port} pid={self.pid} [{status}]>"