#!/usr/bin/env python3
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
wasm Build Script for Function Stream Runtime.

This script compiles the python runtime into a WebAssembly component
using componentize-py. It handles dependency installation, WIT binding
generation, and the final wasm compilation.
"""
import os
import sys
import shutil
import logging
import subprocess
from pathlib import Path

# --- Paths ---
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
WIT_DIR = PROJECT_ROOT / "wit"
PROCESSOR_WIT = WIT_DIR / "processor.wit"
WIT_DEPS_DIR = WIT_DIR / "deps"
WKG_LOCK = WIT_DIR / "wkg.lock"

SRC_DIR = SCRIPT_DIR / "src"
DEPENDENCIES_DIR = SCRIPT_DIR / "dependencies"
TARGET_DIR = SCRIPT_DIR / "target"
WASM_OUTPUT = TARGET_DIR / "functionstream-python-runtime.wasm"

FS_API_DIR = SCRIPT_DIR.parent / "functionstream-api"
FS_API_ADVANCED_DIR = SCRIPT_DIR.parent / "functionstream-api-advanced"

WORLD_NAME = "processor-runtime"
MAIN_MODULE = "fs_runtime.runner"

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("fs_wasm_builder")

class BuildError(Exception):
    pass

class WasmBuilder:
    def __init__(self):
        self.python_exec = sys.executable
        self.componentize_cmd = self._find_tool("componentize-py")
        self.wkg_cmd = shutil.which("wkg")

    def _find_tool(self, name: str) -> str:
        venv_bin = Path(sys.executable).parent
        candidate = venv_bin / name
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)
        path_cmd = shutil.which(name)
        if path_cmd:
            return path_cmd
        raise BuildError(f"Required tool not found: {name}")

    def _ensure_wkg_deps(self):
        if not self.wkg_cmd:
            if shutil.which("cargo"):
                subprocess.run(["cargo", "install", "wkg", "--quiet"], check=True)
                self.wkg_cmd = shutil.which("wkg")
            else:
                raise BuildError("wkg missing")

        staging_dir = Path(subprocess.check_output(["mktemp", "-d"]).decode().strip())
        try:
            tmp_wit_root = staging_dir / "wit"
            tmp_wit_root.mkdir()
            shutil.copy2(PROCESSOR_WIT, tmp_wit_root / "processor.wit")

            result = subprocess.run(
                [self.wkg_cmd, "wit", "fetch"],
                cwd=str(staging_dir),
                capture_output=True,
                text=True
            )

            if result.returncode != 0:
                print(result.stderr, file=sys.stderr)
                raise BuildError(f"wkg fetch failed: {result.stderr}")

            # Update deps directory (source WIT files)
            if WIT_DEPS_DIR.exists():
                shutil.rmtree(WIT_DEPS_DIR)

            tmp_deps = tmp_wit_root / "deps"
            if tmp_deps.exists():
                shutil.copytree(tmp_deps, WIT_DEPS_DIR)

            # Update lock file in wit root
            tmp_lock = staging_dir / "wkg.lock"
            if tmp_lock.exists():
                shutil.copy2(tmp_lock, WKG_LOCK)

        finally:
            shutil.rmtree(staging_dir)

    def prepare_dependencies(self):
        if DEPENDENCIES_DIR.exists():
            shutil.rmtree(DEPENDENCIES_DIR)
        DEPENDENCIES_DIR.mkdir(parents=True, exist_ok=True)
        subprocess.run([
            self.python_exec, "-m", "pip", "install",
            "--target", str(DEPENDENCIES_DIR), str(FS_API_DIR)
        ], check=True, capture_output=True)
        subprocess.run([
            self.python_exec, "-m", "pip", "install",
            "--target", str(DEPENDENCIES_DIR), "--no-deps", str(FS_API_ADVANCED_DIR)
        ], check=True, capture_output=True)

    def build_wasm(self):
        TARGET_DIR.mkdir(parents=True, exist_ok=True)

        # Use the directory containing processor.wit and deps/
        cmd = [
            self.componentize_cmd,
            "-d", str(WIT_DIR.absolute()),
            "-w", WORLD_NAME,
            "componentize",
            "-p", str(DEPENDENCIES_DIR.absolute()),
            "-p", str(SRC_DIR.absolute()),
            MAIN_MODULE,
            "-o", str(WASM_OUTPUT.absolute())
        ]

        result = subprocess.run(cmd, cwd=str(SCRIPT_DIR), capture_output=True, text=True)
        if result.returncode != 0:
            print(result.stderr, file=sys.stderr)
            raise BuildError("componentize-py failed")

        logger.info(f"Generated: {WASM_OUTPUT}")

    def run(self):
        try:
            self._ensure_wkg_deps()
            self.prepare_dependencies()
            self.build_wasm()
        except Exception as e:
            logger.error(str(e))
            sys.exit(1)

if __name__ == "__main__":
    WasmBuilder().run()