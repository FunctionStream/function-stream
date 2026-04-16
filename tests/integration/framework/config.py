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
InstanceConfig: builds and writes the config.yaml consumed by
the FunctionStream binary via FUNCTION_STREAM_CONF.
"""

from pathlib import Path
from typing import Any, Dict

import yaml

from .workspace import InstanceWorkspace

_INTEGRATION_DIR = Path(__file__).resolve().parents[1]
_PROJECT_ROOT = _INTEGRATION_DIR.parents[1]
_SHARED_CACHE_DIR = _INTEGRATION_DIR / "target" / ".shared_cache"


def _find_python_wasm() -> str:
    """Locate the Python WASM runtime for server initialisation."""
    candidates = [
        _PROJECT_ROOT / "python" / "functionstream-runtime" / "target" / "functionstream-python-runtime.wasm",
        _PROJECT_ROOT / "dist" / "function-stream" / "data" / "cache" / "python-runner" / "functionstream-python-runtime.wasm",
    ]
    for c in candidates:
        if c.exists():
            return str(c.resolve())
    return str(candidates[0])


class InstanceConfig:
    """Generates and persists config.yaml for one FunctionStream instance."""

    def __init__(self, host: str, port: int, workspace: InstanceWorkspace):
        self._workspace = workspace
        self._config: Dict[str, Any] = {
            "service": {
                "service_id": f"it-{port}",
                "service_name": "function-stream",
                "version": "1.0.0",
                "host": host,
                "port": port,
                "debug": False,
            },
            "logging": {
                "level": "info",
                "format": "json",
                "file_path": str(workspace.log_dir / "app.log"),
                "max_file_size": 50,
                "max_files": 3,
            },
            "python": {
                "wasm_path": _find_python_wasm(),
                "cache_dir": str(_SHARED_CACHE_DIR / "python-runner"),
                "enable_cache": True,
            },
            "state_storage": {
                "storage_type": "memory",
            },
            "task_storage": {
                "storage_type": "rocksdb",
                "db_path": str(workspace.data_dir / "task"),
            },
            "stream_catalog": {
                "persist": True,
                "db_path": str(workspace.data_dir / "stream_catalog"),
            },
        }

    @property
    def raw(self) -> Dict[str, Any]:
        return self._config

    def override(self, overrides: Dict[str, Any]) -> None:
        """
        Apply overrides using dot-separated keys.
        Example: {"service.debug": True, "logging.level": "debug"}
        """
        for dotted_key, value in overrides.items():
            keys = dotted_key.split(".")
            target = self._config
            for k in keys[:-1]:
                target = target.setdefault(k, {})
            target[keys[-1]] = value

    def write_to_workspace(self) -> Path:
        """Serialize config to the workspace config.yaml and return its path."""
        with open(self._workspace.config_file, "w", encoding="utf-8") as f:
            yaml.dump(self._config, f, default_flow_style=False, sort_keys=False)
        return self._workspace.config_file
