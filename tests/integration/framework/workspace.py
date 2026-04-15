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
    tests/integration/target/<test_name>/<timestamp>/
        conf/config.yaml
        data/
        logs/stdout.log, stderr.log, app.log

After cleanup only ``logs/`` is retained.
"""

import shutil
from datetime import datetime
from pathlib import Path


class InstanceWorkspace:
    """Owns the on-disk directory environment for one FunctionStream instance."""

    def __init__(self, target_dir: Path, test_name: str, port: int):
        self.target_dir = target_dir
        self.test_name = test_name
        self.port = port
        self.timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

        self.root_dir = (
            self.target_dir / self.test_name / self.timestamp
        )
        self.conf_dir = self.root_dir / "conf"
        self.data_dir = self.root_dir / "data"
        self.log_dir = self.root_dir / "logs"

        self.config_file = self.conf_dir / "config.yaml"
        self.stdout_file = self.log_dir / "stdout.log"
        self.stderr_file = self.log_dir / "stderr.log"

    def setup(self) -> None:
        """Create the full directory tree."""
        for d in (self.conf_dir, self.data_dir, self.log_dir):
            d.mkdir(parents=True, exist_ok=True)

    def cleanup(self) -> None:
        """Remove everything except logs/ so only diagnostic output remains."""
        for d in (self.conf_dir, self.data_dir):
            if d.exists():
                shutil.rmtree(d)
