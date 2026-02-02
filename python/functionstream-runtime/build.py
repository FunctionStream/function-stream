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
from typing import List

# --- Configuration ---

# Paths (Relative to this script)
SCRIPT_DIR = Path(__file__).parent.absolute()
PROJECT_ROOT = SCRIPT_DIR.parent.parent
WIT_DIR = PROJECT_ROOT / "wit"
WIT_FILE = WIT_DIR / "processor.wit"

# Source & Artifact Paths
SRC_DIR = SCRIPT_DIR / "src"
DEPENDENCIES_DIR = SCRIPT_DIR / "dependencies"
BINDINGS_DIR = SCRIPT_DIR / "bindings"
TARGET_DIR = SCRIPT_DIR / "target"
WASM_OUTPUT = TARGET_DIR / "functionstream-python-runtime.wasm"

# Sibling Projects
FS_API_DIR = SCRIPT_DIR.parent / "functionstream-api"

# Component Configuration
WORLD_NAME = "processor"
MAIN_MODULE = "fs_runtime.runner"  # The entry point module

# Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("fs_wasm_builder")


class BuildError(Exception):
    """Custom exception for build failures."""


class WasmBuilder:
    """Encapsulates the wasm build process lifecycle."""

    def __init__(self):
        self.python_exec = sys.executable
        self.componentize_cmd = self._find_componentize_py()

    def _find_componentize_py(self) -> str:
        """
        Locates the 'componentize-py' executable.
        Prioritizes the active virtual environment, then system PATH.
        """
        # 1. Check current venv bin (Reliable method)
        venv_bin = Path(sys.executable).parent
        candidate = venv_bin / "componentize-py"
        if candidate.exists() and os.access(candidate, os.X_OK):
            return str(candidate)

        # 2. Check standard PATH (Fallback)
        path_cmd = shutil.which("componentize-py")
        if path_cmd:
            return path_cmd

        logger.error("❌ 'componentize-py' not found.")
        logger.info(
            "Please install it in your environment: pip install componentize-py"
        )
        raise BuildError("Dependency missing: componentize-py")

    def clean(self) -> None:
        """
        Cleans build artifacts and temporary directories.
        CRITICAL: Removes 'bindings' to prevent 'File exists' errors.
        """
        logger.info("Cleaning build artifacts...")

        dirs_to_clean = [TARGET_DIR, DEPENDENCIES_DIR, BINDINGS_DIR]

        for d in dirs_to_clean:
            if d.exists():
                try:
                    shutil.rmtree(d)
                    logger.debug(f"Removed: {d}")
                except OSError as e:
                    logger.warning(f"Failed to remove {d}: {e}")

        logger.info("✓ Clean completed.")

    def prepare_dependencies(self) -> None:
        """Installs local dependencies (functionstream-api) into a temp dir."""
        logger.info("Preparing dependencies...")

        if not FS_API_DIR.exists():
            raise BuildError(
                f"functionstream-api source not found at: {FS_API_DIR}"
            )

        DEPENDENCIES_DIR.mkdir(parents=True, exist_ok=True)

        # Install fs-api to local dependencies folder using pip
        cmd = [
            self.python_exec, "-m", "pip", "install",
            "--target", str(DEPENDENCIES_DIR),
            str(FS_API_DIR)
        ]

        logger.debug(f"Exec: {' '.join(cmd)}")
        try:
            # Note: subprocess.run is safe here as cmd is constructed from
            # controlled build-time inputs, not user-provided data
            subprocess.run(  # noqa: S603
                cmd,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info("✓ Dependencies prepared (fs-api installed).")
        except subprocess.CalledProcessError as e:
            logger.error("Failed to install dependencies.")
            logger.error(f"STDERR: {e.stderr}")
            raise BuildError("Dependency installation failed.")

    def generate_bindings(self) -> None:
        """Generates WIT bindings for reference/development."""
        logger.info("Generating WIT bindings...")

        if not WIT_FILE.exists():
            raise BuildError(f"WIT file not found: {WIT_FILE}")

        # Ensure directory is fresh
        BINDINGS_DIR.mkdir(parents=True, exist_ok=True)

        cmd = [
            self.componentize_cmd,
            "-d", str(WIT_FILE),
            "-w", WORLD_NAME,
            "bindings",
            str(BINDINGS_DIR)
        ]

        logger.debug(f"Exec: {' '.join(cmd)}")
        try:
            subprocess.run(
                cmd,
                cwd=SCRIPT_DIR,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.info(f"✓ Bindings generated at: {BINDINGS_DIR}")
        except subprocess.CalledProcessError as e:
            logger.warning(f"Failed to generate bindings: {e.stderr}")
            logger.warning(
                "Continuing build process (bindings are optional for the artifact)..."
            )

    def build_wasm(self) -> None:
        """Compiles the python code into a wasm component."""
        logger.info("Compiling wasm component...")

        TARGET_DIR.mkdir(parents=True, exist_ok=True)

        # Validate entry point existence
        # Assumes structure: src/fs_runtime/runner.py
        module_path = MAIN_MODULE.replace(".", "/") + ".py"
        entry_file = SRC_DIR / module_path

        if not entry_file.exists():
            raise BuildError(f"Entry point not found: {entry_file}")

        cmd = [
            self.componentize_cmd,
            "-d", str(WIT_FILE),
            "-w", WORLD_NAME,
            "componentize",
            "--stub-wasi",
            "-p", str(DEPENDENCIES_DIR), # Path 1: Pre-installed deps (fs-api)
            "-p", str(SRC_DIR),          # Path 2: Runtime source code
            MAIN_MODULE,                 # Entry module name
            "-o", str(WASM_OUTPUT)
        ]

        logger.info("Executing componentize command...")
        logger.debug(f"Command: {' '.join(cmd)}")

        try:
            # Inherit environment to keep PATH settings
            env = os.environ.copy()

            # Note: subprocess.run is safe here as cmd is constructed from
            # controlled build-time inputs, not user-provided data
            process = subprocess.run(  # noqa: S603
                cmd,
                cwd=SCRIPT_DIR,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                env=env
            )

            # componentize-py outputs helpful info to stdout
            if process.stdout:
                logger.debug(process.stdout)

            if WASM_OUTPUT.exists():
                size_kb = WASM_OUTPUT.stat().st_size / 1024
                logger.info("✓ wasm Build Successful!")
                logger.info(f"  Output: {WASM_OUTPUT}")
                logger.info(f"  Size:   {size_kb:.2f} KB")
            else:
                raise BuildError(
                    "Build command succeeded but output file is missing."
                )

        except subprocess.CalledProcessError as e:
            logger.error("wasm compilation failed.")
            logger.error(f"STDOUT: {e.stdout}")
            logger.error(f"STDERR: {e.stderr}")
            raise BuildError("wasm compilation failed.")

    def run(self) -> None:
        """Main execution flow."""
        print("=" * 60)
        print("   Function Stream Runtime - wasm Builder")
        print("=" * 60)

        try:
            # 1. Clean first (Fixes 'File exists' errors)
            self.clean()

            # 2. Prepare environment
            self.prepare_dependencies()

            # 3. Generate bindings (Optional but good for checking WIT)
            self.generate_bindings()

            # 4. Build actual artifact
            self.build_wasm()

            print("\n" + "=" * 60)
            logger.info("Process completed successfully.")
            print("=" * 60)

        except BuildError as e:
            logger.error(f"Build failed: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            logger.error("Build interrupted.")
            sys.exit(130)
        except Exception:
            logger.exception("Unexpected error occurred:")
            sys.exit(1)


def main():
    builder = WasmBuilder()
    builder.run()


if __name__ == "__main__":
    main()
