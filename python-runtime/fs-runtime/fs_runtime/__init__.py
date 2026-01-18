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
fs-runtime (WASM 内核 & WIT 实现)

包含真实 WIT 绑定，只存在于服务端
"""

__version__ = "1.0.0"

from .runner import (
    WitWorld,
    fs_init,
    fs_process,
    fs_process_watermark,
    fs_take_checkpoint,
    fs_check_heartbeat,
    fs_close,
    fs_exec,
    fs_custom,
)

__all__ = [
    "WitWorld",
    "fs_init",
    "fs_process",
    "fs_process_watermark",
    "fs_take_checkpoint",
    "fs_check_heartbeat",
    "fs_close",
    "fs_exec",
    "fs_custom",
]

