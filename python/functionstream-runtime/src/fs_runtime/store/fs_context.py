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

from typing import Dict, List, Tuple

from fs_api.context import Context
from fs_api.store import (
    Codec,
    KvStore,
    ValueState,
    MapState,
    ListState,
    PriorityQueueState,
    KeyedStateFactory,
    KeyedValueState,
    KeyedMapState,
    KeyedListState,
    KeyedPriorityQueueState,
)

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

    def getOrCreateValueState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> ValueState:
        store = self.getOrCreateKVStore(store_name)
        return ValueState(store, state_name, codec)

    def getOrCreateMapState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> MapState:
        store = self.getOrCreateKVStore(store_name)
        return MapState(store, state_name, key_codec, value_codec)

    def getOrCreateListState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> ListState:
        store = self.getOrCreateKVStore(store_name)
        return ListState(store, state_name, codec)

    def getOrCreatePriorityQueueState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> PriorityQueueState:
        store = self.getOrCreateKVStore(store_name)
        return PriorityQueueState(store, state_name, codec)

    def getOrCreateKeyedStateFactory(self, state_name: str, store_name: str = "__fssdk_structured_state__") -> KeyedStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedStateFactory(store, state_name)

    def getOrCreateKeyedValueState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedValueState:
        factory = self.getOrCreateKeyedStateFactory(state_name, store_name)
        return factory.new_keyed_value(key_codec, value_codec)

    def getOrCreateKeyedMapState(self, state_name: str, key_codec: Codec, map_key_codec: Codec, map_value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedMapState:
        factory = self.getOrCreateKeyedStateFactory(state_name, store_name)
        return factory.new_keyed_map(key_codec, map_key_codec, map_value_codec)

    def getOrCreateKeyedListState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedListState:
        factory = self.getOrCreateKeyedStateFactory(state_name, store_name)
        return factory.new_keyed_list(key_codec, value_codec)

    def getOrCreateKeyedPriorityQueueState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedPriorityQueueState:
        factory = self.getOrCreateKeyedStateFactory(state_name, store_name)
        return factory.new_keyed_priority_queue(key_codec, value_codec)


__all__ = ['WitContext', 'convert_config_to_dict']

