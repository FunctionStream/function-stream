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
Build script for Function Stream API Python Package.

This script handles the lifecycle of building the Python distribution artifacts
(Wheel and Source Distribution) using modern Python packaging standards.
"""

import sys
import shutil
import logging
import subprocess
import importlib.util
from pathlib import Path
from typing import List, Optional

# --- Configuration ---
SCRIPT_DIR = Path(__file__).parent.absolute()
DIST_DIR = SCRIPT_DIR / "dist"
BUILD_DIR = SCRIPT_DIR / "build"
EGG_INFO_PATTERN = "*.egg-info"

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("fs_builder")


class BuildError(Exception):
    """Custom exception for build failures."""
    pass


class Builder:
    """Encapsulates the build logic."""

    def __init__(self, work_dir: Path):
        self.work_dir = work_dir
        self.dist_dir = work_dir / "dist"
        self.python_exec = sys.executable

    def check_environment(self) -> None:
        """
        Verifies that the build environment has necessary tools.
        Raises BuildError if tools are missing.
        """
        logger.info("Checking build environment...")

        required_modules = ["setuptools", "wheel", "build"]
        missing = []

        for mod in required_modules:
            if importlib.util.find_spec(mod) is None:
                missing.append(mod)

        if missing:
            logger.error(f"Missing build dependencies: {', '.join(missing)}")
            logger.info("Please run: pip install --upgrade build setuptools wheel")
            raise BuildError("Environment check failed.")

        logger.info("✓ Environment check passed.")

    def clean(self) -> None:
        """
        Cleans up previous build artifacts.
        """
        logger.info("Cleaning build artifacts...")

        paths_to_clean: List[Path] = [
            self.work_dir / "build",
            self.work_dir / "dist",
            ]

        # Add egg-info directories
        paths_to_clean.extend(self.work_dir.glob(EGG_INFO_PATTERN))

        for path in paths_to_clean:
            if not path.exists():
                continue

            try:
                if path.is_dir():
                    shutil.rmtree(path)
                    logger.debug(f"Removed directory: {path}")
                else:
                    path.unlink()
                    logger.debug(f"Removed file: {path}")
            except OSError as e:
                logger.warning(f"Failed to remove {path}: {e}")

        logger.info("✓ Clean completed.")

    def build(self) -> None:
        """
        Executes the build process using the `build` module.
        """
        logger.info("Building Python package...")

        self.dist_dir.mkdir(parents=True, exist_ok=True)

        cmd = [
            self.python_exec, "-m", "build",
            "--outdir", str(self.dist_dir)
        ]

        logger.info(f"Executing: {' '.join(cmd)}")

        try:
            # Run build command, capturing output but streaming it in case of error
            process = subprocess.run(
                cmd,
                cwd=self.work_dir,
                check=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
            logger.debug(process.stdout)

        except subprocess.CalledProcessError as e:
            logger.error("Build process failed.")
            logger.error(f"STDOUT:\n{e.stdout}")
            logger.error(f"STDERR:\n{e.stderr}")
            raise BuildError("Build command failed.") from e

        self._summarize_artifacts()

    def _summarize_artifacts(self) -> None:
        """Log the results of the build."""
        if not self.dist_dir.exists():
            raise BuildError("Dist directory was not created.")

        artifacts = list(self.dist_dir.glob("*"))
        if not artifacts:
            raise BuildError("No artifacts found in dist directory.")

        logger.info("✓ Build Successful! Generated artifacts:")
        for artifact in sorted(artifacts):
            size_kb = artifact.stat().st_size / 1024
            logger.info(f"  - {artifact.name:<30} ({size_kb:.2f} KB)")

    def verify(self) -> None:
        """
        Performs post-build verification.
        """
        logger.info("Verifying package artifacts...")

        has_wheel = any(self.dist_dir.glob("*.whl"))
        has_sdist = any(self.dist_dir.glob("*.tar.gz"))

        if not has_wheel:
            logger.warning("⚠️ No Wheel (.whl) file found.")
        if not has_sdist:
            logger.warning("⚠️ No Source Distribution (.tar.gz) found.")

        if not (has_wheel or has_sdist):
            raise BuildError("Verification failed: No valid artifacts generated.")

        logger.info("✓ Verification passed.")

    def run(self) -> None:
        """Main execution flow."""
        print("=" * 60)
        print("   Function Stream API - Builder")
        print("=" * 60)

        try:
            self.check_environment()
            self.clean()
            self.build()
            self.verify()

            print("\n" + "=" * 60)
            logger.info("Process completed successfully.")
            print("To install:")
            print(f"  pip install {self.dist_dir}/*.whl")
            print("=" * 60)

        except BuildError as e:
            logger.error(f"Build failed: {e}")
            sys.exit(1)
        except KeyboardInterrupt:
            logger.error("Build interrupted by user.")
            sys.exit(130)
        except Exception as e:
            logger.exception("An unexpected error occurred:")
            sys.exit(1)


def main():
    builder = Builder(work_dir=SCRIPT_DIR)
    builder.run()


if __name__ == "__main__":
    main()