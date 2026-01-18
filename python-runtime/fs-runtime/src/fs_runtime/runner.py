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
fs_runtime.runner

WASM 运行时：直接实现 WIT processor world 的导出函数
"""
import cloudpickle
from typing import Optional, List, Tuple
from functionstream_api.context import Context
from functionstream_api.driver import FSProcessorDriver
from functionstream_api.store import KvStore
from .wit_store import WitKvStore


# --- 全局 Driver 实例 ---
_DRIVER: Optional[FSProcessorDriver] = None
_CONFIG: dict = {}


# ============================================================================
# WIT 接口包装
# ============================================================================

try:
    from wit_world.imports import kv, collector
    _WIT_AVAILABLE = True
except ImportError:
    kv = None
    collector = None
    _WIT_AVAILABLE = False


class WitContext(Context):
    """WIT 环境下的 Context 实现"""
    
    def __init__(self):
        if not _WIT_AVAILABLE:
            raise RuntimeError("WIT environment not available")
        self._collector = collector
        self._store_cache = {}
    
    def emit(self, data: bytes, channel: int = 0):
        """发送数据到输出通道"""
        try:
            self._collector.emit(channel, data)
        except Exception:
            pass
    
    def emit_watermark(self, watermark: int, channel: int = 0):
        """发送水印到输出通道"""
        try:
            self._collector.emit_watermark(channel, watermark)
        except Exception:
            pass
    
    def getOrCreateKVStore(self, name: str) -> KvStore:
        """获取或创建 KV 存储"""
        if name not in self._store_cache:
            store_resource = kv.Store(name)
            self._store_cache[name] = WitKvStore(store_resource)
        return self._store_cache[name]


# ============================================================================
# WIT 导出函数：直接实现 processor world
# ============================================================================

class WitWorld:
    """实现 WIT processor world 的导出函数"""
    
    def fs_init(self, config: List[Tuple[str, str]]) -> None:
        """
        WIT: export fs-init: func(config: list<tuple<string, string>>);
        """
        global _CONFIG
        _CONFIG = dict(config)
        
        # 如果已经有实例，使用新配置重新初始化
        global _DRIVER
        if _DRIVER:
            try:
                ctx = WitContext()
                _DRIVER.init(ctx, _CONFIG)
            except Exception:
                pass
    
    def fs_process(self, source_id: int, data: bytes) -> None:
        """
        WIT: export fs-process: func(source-id: u32, data: list<u8>);
        """
        global _DRIVER
        if not _DRIVER:
            return
        
        ctx = WitContext()
        try:
            _DRIVER.process(ctx, source_id, data)
        except Exception:
            pass
    
    def fs_process_watermark(self, source_id: int, watermark: int) -> None:
        """
        WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
        """
        global _DRIVER
        if not _DRIVER:
            return
        
        ctx = WitContext()
        try:
            _DRIVER.process_watermark(ctx, source_id, watermark)
        except Exception:
            pass
    
    def fs_take_checkpoint(self, checkpoint_id: int) -> bytes:
        """
        WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
        """
        global _DRIVER
        if not _DRIVER:
            return b"{}"
        
        ctx = WitContext()
        try:
            return _DRIVER.take_checkpoint(ctx, checkpoint_id)
        except Exception:
            return b"{}"
    
    def fs_check_heartbeat(self) -> bool:
        """
        WIT: export fs-check-heartbeat: func() -> bool;
        """
        global _DRIVER
        if not _DRIVER:
            return False
        
        ctx = WitContext()
        try:
            return _DRIVER.check_heartbeat(ctx)
        except Exception:
            return False
    
    def fs_close(self) -> None:
        """
        WIT: export fs-close: func();
        """
        global _DRIVER, _CONFIG
        
        if _DRIVER:
            try:
                ctx = WitContext()
                _DRIVER.close(ctx)
            except Exception:
                pass
        
        _DRIVER = None
        _CONFIG = {}
    
    def fs_exec(self, payload: bytes) -> None:
        """
        WIT: export fs-exec: func(payload: list<u8>);
        """
        # fs_exec 已从 FSProcessorDriver 中删除，这里留空
        pass
    
    def fs_custom(self, payload: bytes) -> bytes:
        """
        WIT: export fs-custom: func(payload: list<u8>) -> list<u8>;
        
        用于动态加载 FSProcessorDriver 实例
        """
        global _DRIVER
        
        try:
            # 反序列化 FSProcessorDriver 实例
            _DRIVER = cloudpickle.loads(payload)
            
            # 类型检查
            if not isinstance(_DRIVER, FSProcessorDriver):
                return b"ERR: Not a FSProcessorDriver"
            
            # 初始化
            ctx = WitContext()
            try:
                _DRIVER.init(ctx, _CONFIG)
            except Exception:
                pass
            
            return b"OK"
        except Exception:
            return b"ERR"


# 为了向后兼容，也导出函数形式
def fs_init(config: List[Tuple[str, str]]) -> None:
    """WIT: export fs-init"""
    world = WitWorld()
    world.fs_init(config)


def fs_process(source_id: int, data: bytes) -> None:
    """WIT: export fs-process"""
    world = WitWorld()
    world.fs_process(source_id, data)


def fs_process_watermark(source_id: int, watermark: int) -> None:
    """WIT: export fs-process-watermark"""
    world = WitWorld()
    world.fs_process_watermark(source_id, watermark)


def fs_take_checkpoint(checkpoint_id: int) -> bytes:
    """WIT: export fs-take-checkpoint"""
    world = WitWorld()
    return world.fs_take_checkpoint(checkpoint_id)


def fs_check_heartbeat() -> bool:
    """WIT: export fs-check-heartbeat"""
    world = WitWorld()
    return world.fs_check_heartbeat()


def fs_close() -> None:
    """WIT: export fs-close"""
    world = WitWorld()
    world.fs_close()


def fs_exec(payload: bytes) -> None:
    """WIT: export fs-exec"""
    world = WitWorld()
    world.fs_exec(payload)


def fs_custom(payload: bytes) -> bytes:
    """WIT: export fs-custom"""
    world = WitWorld()
    return world.fs_custom(payload)


__all__ = [
    'WitWorld',
    'fs_init',
    'fs_process',
    'fs_process_watermark',
    'fs_take_checkpoint',
    'fs_check_heartbeat',
    'fs_close',
    'fs_exec',
    'fs_custom',
]

