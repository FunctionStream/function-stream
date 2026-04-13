# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

"""
Pytest conftest: provides a per-test FunctionStreamInstance fixture.
The fixture yields an *un-started* instance so tests can call
.configure() before .start().
"""

import sys
from pathlib import Path

import pytest

# Ensure the framework package is importable from tests/integration/
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from framework import FunctionStreamInstance


@pytest.fixture
def fs_instance(request):
    """
    Provides a fresh FunctionStreamInstance per test function.
    The instance is NOT started automatically — call .start() in the test.
    Teardown always kills the process to avoid leaks.
    """
    test_name = request.node.name
    instance = FunctionStreamInstance(test_name=test_name)
    yield instance
    instance.kill()
