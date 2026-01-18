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
fs_client.packer

序列化工具：打包用户代码，用于传输到 WASM 环境
"""
import cloudpickle
import functionstream_api


def pack(driver_instance) -> bytes:
    """
    打包 StreamDriver 实例
    
    强制 functionstream_api 按引用传递，这样 Payload 到达 WASM 时，
    会使用 WASM 里的 functionstream_api (它会被注入真实工厂)
    
    Args:
        driver_instance: StreamDriver 实例
        
    Returns:
        序列化后的字节数据
    """
    # 强制 functionstream_api 按引用传递
    # cloudpickle 会记录："这个对象类继承自 functionstream_api.core.StreamDriver (引用)"
    # 这样在 WASM 环境中反序列化时，会使用 WASM 环境中的 functionstream_api
    return cloudpickle.dumps(driver_instance)

__all__ = ['pack']

