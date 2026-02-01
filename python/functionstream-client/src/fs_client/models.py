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
Data models for Function Stream Client.

Provides Python-friendly dataclasses for API results, independent of protobuf.
"""

from dataclasses import dataclass
from typing import List, Optional


@dataclass(frozen=True)
class FunctionInfo:
    """Function metadata returned by ShowFunctions."""

    name: str
    task_type: str
    status: str


@dataclass
class ShowFunctionsResult:
    """Result of ShowFunctions RPC: status, message, and list of functions."""

    status_code: int
    message: str
    functions: List[FunctionInfo]
