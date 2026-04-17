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
FunctionStreamProcess: owns the OS-level process lifecycle
(spawn, SIGTERM, SIGKILL) for a single FunctionStream binary.
"""

import logging
import os
import signal
import subprocess
import sys
from pathlib import Path
from typing import IO, Any, Dict, Optional

from .workspace import InstanceWorkspace

logger = logging.getLogger(__name__)


class ProcessManagerError(Exception):
    """Base exception for process management errors."""
    pass


class BinaryNotFoundError(ProcessManagerError):
    """Raised when the target binary executable does not exist."""
    pass


class ProcessAlreadyRunningError(ProcessManagerError):
    """Raised when attempting to start a process that is already running."""
    pass


class FunctionStreamProcess:
    """
    Manages the lifecycle of a single FunctionStream OS process safely.
    Fully cross-platform compatible (Windows, macOS, Linux).
    """

    def __init__(self, binary_path: Path, workspace: InstanceWorkspace):
        self._binary = binary_path.resolve()
        self._workspace = workspace
        self._process: Optional[subprocess.Popen[Any]] = None
        self._stdout_fh: Optional[IO[Any]] = None
        self._stderr_fh: Optional[IO[Any]] = None

    @property
    def pid(self) -> Optional[int]:
        return self._process.pid if self._process else None

    @property
    def is_running(self) -> bool:
        """Check if the process is currently running without blocking."""
        if self._process is None:
            return False
        # poll() returns None if process is still running, else returns exit code
        return self._process.poll() is None

    def start(self) -> None:
        """Launch the binary, redirecting stdout/stderr to log files safely."""
        if self.is_running:
            raise ProcessAlreadyRunningError(
                f"Process is already running with PID {self.pid}."
            )

        if not self._binary.is_file():
            raise BinaryNotFoundError(
                f"Binary not found: {self._binary}. "
                "Run 'make integration-build' first."
            )

        env: Dict[str, str] = os.environ.copy()
        env["FUNCTION_STREAM_CONF"] = str(self._workspace.config_file)
        env["FUNCTION_STREAM_HOME"] = str(self._workspace.root_dir)

        popen_kwargs: Dict[str, Any] = {}
        if sys.platform == "win32":
            popen_kwargs["creationflags"] = subprocess.CREATE_NEW_PROCESS_GROUP
        else:
            popen_kwargs["preexec_fn"] = os.setsid
        try:
            self._stdout_fh = open(self._workspace.stdout_file, "w", encoding="utf-8")
            self._stderr_fh = open(self._workspace.stderr_file, "w", encoding="utf-8")

            self._process = subprocess.Popen(
                [str(self._binary)],
                env=env,
                cwd=str(self._workspace.root_dir),
                stdout=self._stdout_fh,
                stderr=self._stderr_fh,
                **popen_kwargs,
            )
            logger.info("Process started successfully [pid=%d]", self._process.pid)

        except Exception as e:
            self._close_handles()
            logger.error("Failed to start process: %s", e)
            raise ProcessManagerError(f"Process initialization failed: {e}") from e

    def stop(self, timeout: float = 10.0) -> None:
        """
        Graceful shutdown via SIGTERM (POSIX) or CTRL_BREAK (Windows).
        Falls back to immediate Kill on timeout.
        """
        if not self.is_running or self._process is None:
            self._close_handles()
            return

        logger.debug("Attempting graceful shutdown [pid=%d]...", self._process.pid)
        try:
            self._send_termination_signal()
            self._process.wait(timeout=timeout)
            logger.info("Process stopped gracefully [pid=%d].", self._process.pid)
        except (ProcessLookupError, PermissionError):
            # Process already died or OS denied access
            pass
        except subprocess.TimeoutExpired:
            logger.warning("Graceful shutdown timed out (%.1fs). Escalating to Kill [pid=%d].",
                           timeout, self._process.pid)
            self.kill()
            return
        finally:
            self._close_handles()

    def kill(self) -> None:
        """Immediately terminate the process group across all platforms."""
        if not self.is_running or self._process is None:
            self._close_handles()
            return

        logger.debug("Executing force kill [pid=%d]...", self._process.pid)
        try:
            self._send_kill_signal()
            self._process.wait(timeout=5.0)
            logger.info("Process forcefully killed [pid=%d].", self._process.pid)
        except (ProcessLookupError, PermissionError):
            pass
        except subprocess.TimeoutExpired:
            logger.error("Process group kill timed out. Falling back to base kill.")
            self._process.kill()
            self._process.wait(timeout=2.0)
        finally:
            self._close_handles()

    # ------------------------------------------------------------------
    # OS-Specific Signal Implementations
    # ------------------------------------------------------------------

    def _send_termination_signal(self) -> None:
        """Cross-platform implementation for graceful termination."""
        if sys.platform == "win32":
            self._process.send_signal(signal.CTRL_BREAK_EVENT)
        else:
            os.killpg(os.getpgid(self._process.pid), signal.SIGTERM)

    def _send_kill_signal(self) -> None:
        if sys.platform == "win32":
            self._process.kill()
        else:
            os.killpg(os.getpgid(self._process.pid), signal.SIGKILL)

    def _close_handles(self) -> None:
        """Safely close open file handles and clear state."""
        for fh in (self._stdout_fh, self._stderr_fh):
            if fh is not None and not fh.closed:
                try:
                    fh.close()
                except Exception as e:
                    logger.debug("Failed to close file handle: %s", e)

        self._stdout_fh = None
        self._stderr_fh = None
        self._process = None