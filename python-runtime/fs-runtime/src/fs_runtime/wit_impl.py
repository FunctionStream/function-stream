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
fs_runtime.wit_impl

WIT 实现：将 processor.wit 接口包装为 Python 实现
"""
from typing import Optional
try:
    from wit_world.imports import kv, collector
except ImportError:
    kv = None
    collector = None

from functionstream_api.interfaces import StateBackend, CollectorBackend
from functionstream_api.factory import BackendFactory


# --- 真实 WIT 实现 ---
class WitState(StateBackend):
    """WIT 状态存储：包装 processor.wit 中的 kv.store 接口"""
    
    def __init__(self, store_resource):
        """
        初始化 WIT Store
        
        Args:
            store_resource: WIT kv.Store 资源实例
        """
        self._store = store_resource
    
    def get(self, key: bytes) -> Optional[bytes]:
        """
        获取状态（包装 WIT get-state）
        
        processor.wit: get-state: func(key: list<u8>) -> result<option<list<u8>>, error>
        """
        try:
            result = self._store.get_state(key)
            # 处理 result 类型
            if result is None:
                return None
            if hasattr(result, 'is_err'):
                if result.is_err():
                    return None
                if hasattr(result, 'unwrap'):
                    option_value = result.unwrap()
                    if option_value is None:
                        return None
                    if hasattr(option_value, 'unwrap'):
                        return option_value.unwrap()
                    return option_value
            return result if result is not None else None
        except Exception:
            return None
    
    def put(self, key: bytes, val: bytes):
        """
        存储状态（包装 WIT put-state）
        
        processor.wit: put-state: func(key: list<u8>, value: list<u8>) -> result<_, error>
        """
        try:
            result = self._store.put_state(key, val)
            if hasattr(result, 'is_err') and result.is_err():
                raise RuntimeError("Put state failed")
        except Exception:
            raise


class WitCollector(CollectorBackend):
    """WIT 收集器：包装 processor.wit 中的 collector 接口"""
    
    def __init__(self, collector_interface):
        """
        初始化 WIT Collector
        
        Args:
            collector_interface: WIT collector 接口实例
        """
        self._collector = collector_interface
    
    def emit(self, ch: int, data: bytes):
        """
        发送数据（包装 WIT emit）
        
        processor.wit: emit: func(target-id: u32, data: list<u8>)
        """
        try:
            self._collector.emit(ch, data)
        except Exception:
            raise


class WasmFactory(BackendFactory):
    """WASM 工厂：创建 WIT 后端"""
    
    def __init__(self):
        """初始化 WASM 工厂，创建 WIT 资源"""
        if kv is None or collector is None:
            raise RuntimeError("WIT environment not available")
        # 创建 KV store (根据 processor.wit，store 需要 name 参数构造)
        self._store_resource = kv.Store("default")
        self._collector_interface = collector
    
    def create_state_backend(self) -> StateBackend:
        """创建 WIT 状态存储后端"""
        return WitState(self._store_resource)
    
    def create_collector_backend(self) -> CollectorBackend:
        """创建 WIT 收集器后端"""
        return WitCollector(self._collector_interface)

__all__ = ['WitState', 'WitCollector', 'WasmFactory']

