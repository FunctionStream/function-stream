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
InstanceWorkspace: manages the directory tree for a single
FunctionStream test instance.

Layout (during test run):
    tests/integration/target/<test_name>/<timestamp>-<port>-<uuid>/
        conf/config.yaml
        data/
        logs/stdout.log, stderr.log, app.log

After cleanup only ``logs/`` is retained.
"""

import logging
import shutil
import time
import uuid
from datetime import datetime
from pathlib import Path

logger = logging.getLogger(__name__)


class WorkspaceError(Exception):
    """Base exception for workspace-related errors."""
    pass


class InstanceWorkspace:
    """
    Owns the on-disk directory environment for one FunctionStream instance.
    Designed for safe parallel execution and cross-platform robustness.
    """

    def __init__(self, target_dir: Path, test_name: str, port: int):
        self.target_dir = target_dir.resolve()
        self.test_name = test_name
        self.port = port

        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        unique_id = uuid.uuid4().hex[:6]
        self.instance_id = f"{timestamp}-{port}-{unique_id}"

        self.root_dir = self.target_dir / self.test_name / self.instance_id

        self.conf_dir = self.root_dir / "conf"
        self.data_dir = self.root_dir / "data"
        self.log_dir = self.root_dir / "logs"

        self.config_file = self.conf_dir / "config.yaml"
        self.stdout_file = self.log_dir / "stdout.log"
        self.stderr_file = self.log_dir / "stderr.log"

    def setup(self) -> None:
        """Create the full directory tree safely."""
        logger.debug("Setting up workspace at %s", self.root_dir)
        try:
            for d in (self.conf_dir, self.data_dir, self.log_dir):
                d.mkdir(parents=True, exist_ok=True)
        except OSError as e:
            logger.error("Failed to create workspace directories: %s", e)
            raise WorkspaceError(f"Workspace setup failed: {e}") from e

    def cleanup(self) -> None:
        """
        Remove everything except logs/ so only diagnostic output remains.
        Uses a robust deletion strategy to handle temporary OS file locks.
        """
        logger.debug("Cleaning up workspace (retaining logs) at %s", self.root_dir)
        for d in (self.conf_dir, self.data_dir):
            if d.exists():
                self._safe_rmtree(d)

    def _safe_rmtree(self, path: Path, retries: int = 3, delay: float = 0.5) -> None:
        """
        Robust directory removal with backoff retries.
        Crucial for Windows where RocksDB/LevelDB files might hold residual locks
        for a few milliseconds after the parent process dies.
        """
        for attempt in range(1, retries + 1):
            try:
                shutil.rmtree(path, ignore_errors=False)
                return
            except OSError as e:
                if attempt < retries:
                    logger.debug(
                        "File lock detected during cleanup of %s (attempt %d/%d). Retrying in %ss...",
                        path, attempt, retries, delay
                    )
                    time.sleep(delay)
                else:
                    logger.warning(
                        "Failed to completely remove %s after %d attempts. "
                        "Leaving residual files. Error: %s",
                        path, retries, e
                    )
                    shutil.rmtree(path, ignore_errors=True)