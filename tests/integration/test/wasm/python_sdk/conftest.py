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
Fixtures for Python SDK integration tests.
A single FunctionStreamInstance is shared across the entire module.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from framework import FunctionStreamInstance

PROJECT_ROOT = Path(__file__).resolve().parents[5]
PYTHON_EXAMPLE_DIR = PROJECT_ROOT / "examples" / "python-processor"


@pytest.fixture(scope="session")
def fs_server():
    """Start a FunctionStream server once for all Python SDK tests."""
    instance = FunctionStreamInstance(test_name="wasm_python_sdk")
    instance.start()
    yield instance
    instance.kill()


@pytest.fixture(scope="session")
def python_example_dir():
    """Path to the Python processor example directory."""
    return PYTHON_EXAMPLE_DIR
