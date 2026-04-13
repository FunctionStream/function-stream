# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
FunctionStreamInstance: the Facade that composes workspace, config,
and process into a single easy-to-use API for test cases.
"""

import logging
from pathlib import Path
from typing import Any, Optional

from .config import InstanceConfig
from .process import FunctionStreamProcess
from .utils import find_free_port, wait_for_port
from .workspace import InstanceWorkspace

logger = logging.getLogger(__name__)

_INTEGRATION_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = _INTEGRATION_DIR.parents[1]
_TARGET_DIR = _INTEGRATION_DIR / "target"
_BINARY_PATH = _PROJECT_ROOT / "target" / "release" / "function-stream"


class FunctionStreamInstance:
    """
    Facade for a single FunctionStream server used in integration tests.

    Usage:
        inst = FunctionStreamInstance("my_test")
        inst.configure(**{"service.debug": True}).start()
        client = inst.get_client()
        ...
        inst.kill()
    """

    def __init__(
        self,
        test_name: str = "unnamed",
        host: str = "127.0.0.1",
        binary_path: Optional[Path] = None,
    ):
        self.host = host
        self.port = find_free_port()

        binary = binary_path or _BINARY_PATH

        self.workspace = InstanceWorkspace(_TARGET_DIR, test_name, self.port)
        self.config = InstanceConfig(self.host, self.port, self.workspace)
        self.process = FunctionStreamProcess(binary, self.workspace)

    # ------------------------------------------------------------------
    # Properties
    # ------------------------------------------------------------------

    @property
    def is_running(self) -> bool:
        return self.process.is_running

    @property
    def pid(self):
        return self.process.pid

    # ------------------------------------------------------------------
    # Configuration (before start)
    # ------------------------------------------------------------------

    def configure(self, **overrides: Any) -> "FunctionStreamInstance":
        """Apply config overrides. Chainable: inst.configure(k=v).start()."""
        self.config.override(overrides)
        return self

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self, timeout: float = 30.0) -> "FunctionStreamInstance":
        """Prepare workspace, write config, launch binary, wait for ready."""
        if self.is_running:
            return self

        self.workspace.setup()
        self.config.write_to_workspace()

        logger.info(
            "Starting FunctionStream [port=%d, dir=%s]",
            self.port,
            self.workspace.root_dir,
        )
        self.process.start()

        if not wait_for_port(self.host, self.port, timeout):
            stderr_tail = self._read_tail(self.workspace.stderr_file)
            self.process.kill()
            raise RuntimeError(
                f"Server did not become ready within {timeout}s on port {self.port}.\n"
                f"stderr tail:\n{stderr_tail}"
            )

        logger.info(
            "FunctionStream ready [port=%d, pid=%d]",
            self.port,
            self.process.pid,
        )
        return self

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful SIGTERM shutdown."""
        self.process.stop(timeout=timeout)

    def kill(self) -> None:
        """Immediate SIGKILL."""
        self.process.kill()

    def restart(self, timeout: float = 10.0) -> "FunctionStreamInstance":
        """Stop then start (same port, same workspace)."""
        self.stop(timeout=timeout)
        return self.start()

    # ------------------------------------------------------------------
    # Client access
    # ------------------------------------------------------------------

    def get_client(self):
        """Return a connected FsClient. Caller should close it when done."""
        try:
            from fs_client import FsClient
        except ImportError:
            raise ImportError(
                "functionstream-client is not installed. "
                "Run: pip install -e python/functionstream-client"
            )
        return FsClient(host=self.host, port=self.port)

    def execute_sql(self, sql: str, timeout: float = 30.0):
        """Convenience helper: send a SQL statement via gRPC ExecuteSql."""
        try:
            import grpc
            from fs_client._proto import function_stream_pb2, function_stream_pb2_grpc
        except ImportError:
            raise ImportError(
                "functionstream-client is not installed. "
                "Run: pip install -e python/functionstream-client"
            )

        channel = grpc.insecure_channel(f"{self.host}:{self.port}")
        try:
            stub = function_stream_pb2_grpc.FunctionStreamServiceStub(channel)
            request = function_stream_pb2.SqlRequest(sql=sql)
            return stub.ExecuteSql(request, timeout=timeout)
        finally:
            channel.close()

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    @staticmethod
    def _read_tail(path: Path, chars: int = 2000) -> str:
        if not path.exists():
            return "<file not found>"
        text = path.read_text(errors="replace")
        return text[-chars:] if len(text) > chars else text

    def __repr__(self) -> str:
        status = "running" if self.is_running else "stopped"
        return f"<FunctionStreamInstance port={self.port} pid={self.pid} {status}>"

    def __del__(self):
        if hasattr(self, "process") and self.process.is_running:
            self.process.kill()
