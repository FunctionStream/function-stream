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

from typing import List, Tuple, Dict

from functionstream_api.context import Context
from functionstream_api.store import KvStore

from .fs_collector import emit, emit_watermark
from .fs_store import FSStore


def convert_config_to_dict(config: List[Tuple[str, str]]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    
    if not config:
        return result
    
    for item in config:
        if isinstance(item, (list, tuple)):
            if len(item) >= 2:
                key = str(item[0])
                value = str(item[1])
                result[key] = value
        elif isinstance(item, dict):
            for k, v in item.items():
                result[str(k)] = str(v)
        else:
            continue
    
    return result


class WitContext(Context):
    
    def __init__(self, config: Dict[str, str] = None):
        self._store_cache: dict[str, KvStore] = {}
        self._CONFIG: Dict[str, str] = config.copy() if config is not None else {}
    
    def emit(self, data: bytes, channel: int = 0) -> None:
        emit(data, channel)
    
    def emit_watermark(self, watermark: int, channel: int = 0) -> None:
        emit_watermark(watermark, channel)
    
    def getOrCreateKVStore(self, name: str) -> KvStore:
        if name not in self._store_cache:
            self._store_cache[name] = FSStore(name)
        return self._store_cache[name]
    
    def getConfig(self) -> Dict[str, str]:
        return self._CONFIG.copy()


__all__ = ['WitContext', 'convert_config_to_dict']

