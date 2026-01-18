# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
functionstream-client (开发者工具 & Mock 实现)

包含 Mock 实现，用于本地开发和序列化
"""

__version__ = "1.0.0"

from .mocks import init_local_env, MockState, MockCollector, LocalFactory
from .packer import pack

__all__ = [
    "init_local_env",
    "MockState",
    "MockCollector",
    "LocalFactory",
    "pack",
]


