# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
FunctionStreamProcess: owns the OS-level process lifecycle
(spawn, SIGTERM, SIGKILL) for a single FunctionStream binary.
"""

import logging
import os
import signal
import subprocess
from pathlib import Path
from typing import Optional

from .workspace import InstanceWorkspace

logger = logging.getLogger(__name__)


class FunctionStreamProcess:
    """Manages the lifecycle of a single FunctionStream OS process."""

    def __init__(self, binary_path: Path, workspace: InstanceWorkspace):
        self._binary = binary_path
        self._workspace = workspace
        self._process: Optional[subprocess.Popen] = None
        self._stdout_fh = None
        self._stderr_fh = None

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    def start(self) -> None:
        """Launch the binary, redirecting stdout/stderr to log files."""
        if not self._binary.exists():
            raise FileNotFoundError(
                f"Binary not found: {self._binary}. "
                "Run 'make integration-build' first."
            )

        env = os.environ.copy()
        env["FUNCTION_STREAM_CONF"] = str(self._workspace.config_file)
        env["FUNCTION_STREAM_HOME"] = str(self._workspace.root_dir)

        self._stdout_fh = open(self._workspace.stdout_file, "w")
        self._stderr_fh = open(self._workspace.stderr_file, "w")

        self._process = subprocess.Popen(
            [str(self._binary)],
            env=env,
            cwd=str(self._workspace.root_dir),
            stdout=self._stdout_fh,
            stderr=self._stderr_fh,
            preexec_fn=os.setsid if os.name != "nt" else None,
        )
        logger.info("Process started [pid=%d]", self._process.pid)

    def stop(self, timeout: float = 10.0) -> None:
        """Graceful shutdown via SIGTERM, falls back to SIGKILL on timeout."""
        if not self.is_running:
            self._close_handles()
            return

        logger.info("Sending SIGTERM [pid=%d]", self._process.pid)
        try:
            os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)
            self._process.wait(timeout=timeout)
        except (ProcessLookupError, PermissionError):
            pass
        except subprocess.TimeoutExpired:
            logger.warning("SIGTERM timed out, escalating to SIGKILL [pid=%d]", self._process.pid)
            self.kill()
            return

        self._close_handles()

    def kill(self) -> None:
        """Immediately SIGKILL the process group."""
        if not self.is_running:
            self._close_handles()
            return

        logger.info("Sending SIGKILL [pid=%d]", self._process.pid)
        try:
            os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)
            self._process.wait(timeout=5)
        except (ProcessLookupError, PermissionError):
            pass
        except subprocess.TimeoutExpired:
            self._process.kill()
            self._process.wait(timeout=5)

        self._close_handles()

    def _close_handles(self) -> None:
        for fh in (self._stdout_fh, self._stderr_fh):
            if fh and not fh.closed:
                fh.close()
        self._stdout_fh = None
        self._stderr_fh = None
        self._process = None
