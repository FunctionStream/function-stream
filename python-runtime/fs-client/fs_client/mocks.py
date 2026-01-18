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
fs_client.mocks

Mock 实现：实现 fs-api 定义的接口，用于本地开发
"""
from typing import Optional
from functionstream_api.interfaces import StateBackend, CollectorBackend
from functionstream_api.factory import BackendFactory, set_factory


# --- Mock 实现 ---
class MockState(StateBackend):
    """Mock 状态存储：本地内存存储"""
    
    def __init__(self):
        self.data = {}
    
    def get(self, key: bytes) -> Optional[bytes]:
        """获取状态"""
        return self.data.get(key)
    
    def put(self, key: bytes, val: bytes):
        """存储状态"""
        self.data[key] = val
    
    def delete(self, key: bytes):
        """删除状态（可选实现）"""
        if key in self.data:
            del self.data[key]


class MockCollector(CollectorBackend):
    """Mock 收集器：本地调试用，将数据存到缓冲区"""
    
    def __init__(self):
        self.buffer = []
    
    def emit(self, ch: int, data: bytes):
        """
        发送数据
        
        本地调试可以将数据存起来供断言检查
        """
        self.buffer.append((ch, data))


class LocalFactory(BackendFactory):
    """本地工厂：创建 Mock 后端"""
    
    def create_state_backend(self) -> StateBackend:
        """创建 Mock 状态存储后端"""
        return MockState()
    
    def create_collector_backend(self) -> CollectorBackend:
        """创建 Mock 收集器后端"""
        return MockCollector()


# --- 初始化钩子 ---
def init_local_env():
    """
    本地开发时调用，注入 Mock 工厂
    
    调用此函数后，functionstream_api.Context 将使用 Mock 后端
    """
    set_factory(LocalFactory())

__all__ = ['MockState', 'MockCollector', 'LocalFactory', 'init_local_env']

