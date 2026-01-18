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
fs_runtime.app

WIT 导出函数的入口点

这个文件包含 WitWorld 类，实现 WIT processor world 的导出函数。
componentize-py 会扫描这个文件并生成相应的 WIT 绑定。
"""

from typing import List, Tuple
from .runner import (
    kernel_init,
    kernel_custom,
    kernel_process,
    kernel_process_watermark,
    kernel_take_checkpoint,
    kernel_check_heartbeat,
    kernel_close,
)


class WitWorld:
    """实现 WIT processor world 的导出函数"""
    
    def fs_init(self, config: List[Tuple[str, str]]) -> None:
        """
        WIT: export fs-init: func(config: list<tuple<string, string>>);
        """
        kernel_init(config)
    
    def fs_process(self, source_id: int, data: bytes) -> None:
        """
        WIT: export fs-process: func(source-id: u32, data: list<u8>);
        """
        kernel_process(source_id, data)
    
    def fs_custom(self, payload: bytes) -> bytes:
        """
        WIT: export fs-custom: func(payload: list<u8>) -> list<u8>;
        """
        return kernel_custom(payload)
    
    def fs_process_watermark(self, source_id: int, watermark: int) -> None:
        """
        WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
        """
        kernel_process_watermark(source_id, watermark)
    
    def fs_take_checkpoint(self, checkpoint_id: int) -> bytes:
        """
        WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
        """
        return kernel_take_checkpoint(checkpoint_id)
    
    def fs_check_heartbeat(self) -> bool:
        """
        WIT: export fs-check-heartbeat: func() -> bool;
        """
        return kernel_check_heartbeat()
    
    def fs_close(self) -> None:
        """
        WIT: export fs-close: func();
        """
        kernel_close()


# 为了向后兼容，也导出函数形式（如果有代码直接导入）
def fs_init(config):
    """WIT: export fs-init"""
    kernel_init(config)


def fs_process(source_id: int, data: bytes):
    """WIT: export fs-process"""
    kernel_process(source_id, data)


def fs_custom(payload: bytes) -> bytes:
    """WIT: export fs-custom"""
    return kernel_custom(payload)


def fs_process_watermark(source_id: int, watermark: int):
    """WIT: export fs-process-watermark"""
    kernel_process_watermark(source_id, watermark)


def fs_take_checkpoint(checkpoint_id: int) -> bytes:
    """WIT: export fs-take-checkpoint"""
    return kernel_take_checkpoint(checkpoint_id)


def fs_check_heartbeat() -> bool:
    """WIT: export fs-check-heartbeat"""
    return kernel_check_heartbeat()


def fs_close():
    """WIT: export fs-close"""
    kernel_close()

__all__ = [
    'WitWorld',
    'fs_init',
    'fs_process',
    'fs_custom',
    'fs_process_watermark',
    'fs_take_checkpoint',
    'fs_check_heartbeat',
    'fs_close',
]

