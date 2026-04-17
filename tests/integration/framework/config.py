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

import logging
import os
import tempfile
from pathlib import Path
from typing import Any, Dict, List

import yaml

from .workspace import InstanceWorkspace

logger = logging.getLogger(__name__)


class ConfigurationError(Exception):
    """Base exception for all configuration-related errors."""
    pass


class WasmRuntimeNotFoundError(ConfigurationError):
    """Raised when the required Python WASM runtime cannot be located."""
    pass


class PathManager:
    """
    Utility class for resolving and managing cross-platform paths.
    All outputs are standardized to POSIX format for safe YAML serialization.
    """

    _INTEGRATION_DIR = Path(__file__).resolve().parents[1]
    _PROJECT_ROOT = _INTEGRATION_DIR.parents[1]

    @classmethod
    def get_target_dir(cls) -> Path:
        """Return the integration-test workspace root (tests/integration/target)."""
        return cls._INTEGRATION_DIR / "target"

    @classmethod
    def get_binary_path(cls) -> Path:
        """Locate the compiled function-stream binary under the project target dir."""
        import platform
        import struct

        system = platform.system()
        machine = platform.machine().lower()
        arch_map = {"arm64": "aarch64", "amd64": "x86_64"}
        arch = arch_map.get(machine, machine)

        if system == "Linux":
            triple = f"{arch}-unknown-linux-gnu"
        elif system == "Darwin":
            triple = f"{arch}-apple-darwin"
        elif system == "Windows":
            triple = f"{arch}-pc-windows-msvc"
        else:
            triple = f"{arch}-unknown-linux-gnu"

        binary_name = "function-stream.exe" if system == "Windows" else "function-stream"
        candidate = cls._PROJECT_ROOT / "target" / triple / "release" / binary_name

        if candidate.is_file():
            return candidate

        fallback = cls._PROJECT_ROOT / "target" / "release" / binary_name
        if fallback.is_file():
            return fallback

        raise FileNotFoundError(
            f"Cannot find function-stream binary. Checked:\n"
            f"  - {candidate}\n"
            f"  - {fallback}\n"
            f"Build first with: make build (or make build-lite)"
        )

    @classmethod
    def get_shared_cache_dir(cls) -> Path:
        return cls._INTEGRATION_DIR / "target" / ".shared_cache"

    @classmethod
    def build_posix_path(cls, base: Path, *segments: str) -> str:
        """
        Safely joins a base Path with string segments and returns a POSIX-compliant string.
        Eliminates manual string concatenation and Windows backslash issues.
        """
        target_path = base
        for segment in segments:
            target_path = target_path / segment
        return target_path.as_posix()

    @classmethod
    def find_python_wasm_posix(cls) -> str:
        """Locates the Python WASM runtime and returns its POSIX path."""
        candidates: List[Path] = [
            cls._PROJECT_ROOT / "python" / "functionstream-runtime" / "target" / "functionstream-python-runtime.wasm",
            cls._PROJECT_ROOT / "dist" / "function-stream" / "data" / "cache" / "python-runner" / "functionstream-python-runtime.wasm",
            ]

        for candidate in candidates:
            if candidate.is_file():
                logger.debug("Found Python WASM runtime at: %s", candidate)
                return candidate.resolve().as_posix()

        raise WasmRuntimeNotFoundError(
            "Could not find python-runtime.wasm. Checked locations:\n" +
            "\n".join(f" - {c}" for c in candidates)
        )


class InstanceConfig:
    """
    Generates and persists config.yaml for one FunctionStream instance.
    """

    def __init__(self, host: str, port: int, workspace: InstanceWorkspace):
        self._workspace = workspace

        wasm_path = PathManager.find_python_wasm_posix()
        cache_dir = PathManager.build_posix_path(PathManager.get_shared_cache_dir(), "python-runner")
        log_file = PathManager.build_posix_path(workspace.log_dir, "app.log")
        task_db_path = PathManager.build_posix_path(workspace.data_dir, "task")
        catalog_db_path = PathManager.build_posix_path(workspace.data_dir, "stream_catalog")

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
                "file_path": log_file,
                "max_file_size": 50,
                "max_files": 3,
            },
            "python": {
                "wasm_path": wasm_path,
                "cache_dir": cache_dir,
                "enable_cache": True,
            },
            "state_storage": {
                "storage_type": "memory",
            },
            "task_storage": {
                "storage_type": "rocksdb",
                "db_path": task_db_path,
            },
            "stream_catalog": {
                "persist": True,
                "db_path": catalog_db_path,
            },
        }

    @property
    def raw(self) -> Dict[str, Any]:
        return self._config.copy()

    def override(self, overrides: Dict[str, Any]) -> None:
        """Apply overrides using dot-separated keys."""
        for dotted_key, value in overrides.items():
            keys = dotted_key.split(".")
            target = self._config

            for k in keys[:-1]:
                target = target.setdefault(k, {})
                if not isinstance(target, dict):
                    raise ConfigurationError(f"Cannot override key '{dotted_key}': '{k}' is not a dictionary.")

            target[keys[-1]] = value

    def write_to_workspace(self) -> Path:
        """Serialize config to the workspace config.yaml safely."""
        target_file = Path(self._workspace.config_file)
        target_file.parent.mkdir(parents=True, exist_ok=True)

        fd, temp_path = tempfile.mkstemp(
            dir=target_file.parent,
            prefix=".config-",
            suffix=".yaml",
            text=True
        )

        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                yaml.safe_dump(
                    self._config,
                    f,
                    default_flow_style=False,
                    sort_keys=False
                )
            os.replace(temp_path, target_file)
        except Exception as e:
            Path(temp_path).unlink(missing_ok=True)
            logger.error("Failed to write configuration file: %s", e)
            raise ConfigurationError(f"Configuration write failed: {e}") from e

        return target_file