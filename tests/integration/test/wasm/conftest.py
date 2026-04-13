# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from framework import FunctionStreamInstance


@pytest.fixture
def fs_instance(request):
    """Provides a fresh FunctionStreamInstance per test. Not started automatically."""
    instance = FunctionStreamInstance(test_name=request.node.name)
    yield instance
    instance.kill()
