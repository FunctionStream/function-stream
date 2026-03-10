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
    AggregatingState,
    ReducingState,
    KeyedListStateFactory,
    KeyedValueStateFactory,
    KeyedMapStateFactory,
    KeyedPriorityQueueStateFactory,
    KeyedAggregatingStateFactory,
    KeyedReducingStateFactory,
    PickleCodec,
    BytesCodec,
    IntCodec,
    default_codec_for,
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

    def getOrCreateValueState(self, store_name: str, codec: Codec) -> ValueState:
        store = self.getOrCreateKVStore(store_name)
        return ValueState(store, codec)

    def getOrCreateValueStateAutoCodec(self, store_name: str) -> ValueState:
        store = self.getOrCreateKVStore(store_name)
        return ValueState(store, PickleCodec())

    def getOrCreateMapState(self, store_name: str, key_codec: Codec, value_codec: Codec) -> MapState:
        store = self.getOrCreateKVStore(store_name)
        return MapState(store, key_codec, value_codec)

    def getOrCreateMapStateAutoKeyCodec(self, store_name: str, value_codec: Codec) -> MapState:
        store = self.getOrCreateKVStore(store_name)
        return MapState(store, BytesCodec(), value_codec)

    def getOrCreateListState(self, store_name: str, codec: Codec) -> ListState:
        store = self.getOrCreateKVStore(store_name)
        return ListState(store, codec)

    def getOrCreateListStateAutoCodec(self, store_name: str) -> ListState:
        store = self.getOrCreateKVStore(store_name)
        return ListState(store, PickleCodec())

    def getOrCreatePriorityQueueState(self, store_name: str, codec: Codec) -> PriorityQueueState:
        store = self.getOrCreateKVStore(store_name)
        return PriorityQueueState(store, codec)

    def getOrCreatePriorityQueueStateAutoCodec(self, store_name: str) -> PriorityQueueState:
        store = self.getOrCreateKVStore(store_name)
        return PriorityQueueState(store, IntCodec())

    def getOrCreateAggregatingState(
        self, store_name: str, acc_codec: Codec, agg_func: object
    ) -> AggregatingState:
        store = self.getOrCreateKVStore(store_name)
        return AggregatingState(store, acc_codec, agg_func)

    def getOrCreateAggregatingStateAutoCodec(
        self, store_name: str, agg_func: object
    ) -> AggregatingState:
        store = self.getOrCreateKVStore(store_name)
        return AggregatingState(store, PickleCodec(), agg_func)

    def getOrCreateReducingState(
        self, store_name: str, value_codec: Codec, reduce_func: object
    ) -> ReducingState:
        store = self.getOrCreateKVStore(store_name)
        return ReducingState(store, value_codec, reduce_func)

    def getOrCreateReducingStateAutoCodec(
        self, store_name: str, reduce_func: object
    ) -> ReducingState:
        store = self.getOrCreateKVStore(store_name)
        return ReducingState(store, PickleCodec(), reduce_func)

    def getOrCreateKeyedListStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedListStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedListStateFactory(store, namespace, key_group, value_codec)

    def getOrCreateKeyedListStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type=None
    ) -> KeyedListStateFactory:
        store = self.getOrCreateKVStore(store_name)
        codec = default_codec_for(value_type) if value_type is not None else PickleCodec()
        return KeyedListStateFactory(store, namespace, key_group, codec)

    def getOrCreateKeyedValueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedValueStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedValueStateFactory(store, namespace, key_group, value_codec)

    def getOrCreateKeyedValueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type=None
    ) -> KeyedValueStateFactory:
        store = self.getOrCreateKVStore(store_name)
        codec = default_codec_for(value_type) if value_type is not None else PickleCodec()
        return KeyedValueStateFactory(store, namespace, key_group, codec)

    def getOrCreateKeyedMapStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, key_codec: Codec, value_codec: Codec
    ) -> KeyedMapStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedMapStateFactory(store, namespace, key_group, key_codec, value_codec)

    def getOrCreateKeyedMapStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedMapStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedMapStateFactory(store, namespace, key_group, BytesCodec(), value_codec)

    def getOrCreateKeyedPriorityQueueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, item_codec: Codec
    ) -> KeyedPriorityQueueStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedPriorityQueueStateFactory(store, namespace, key_group, item_codec)

    def getOrCreateKeyedPriorityQueueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, item_type=None
    ) -> KeyedPriorityQueueStateFactory:
        store = self.getOrCreateKVStore(store_name)
        codec = default_codec_for(item_type) if item_type is not None else IntCodec()
        return KeyedPriorityQueueStateFactory(store, namespace, key_group, codec)

    def getOrCreateKeyedAggregatingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, acc_codec: Codec, agg_func: object
    ) -> KeyedAggregatingStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedAggregatingStateFactory(store, namespace, key_group, acc_codec, agg_func)

    def getOrCreateKeyedAggregatingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, agg_func: object, acc_type=None
    ) -> KeyedAggregatingStateFactory:
        store = self.getOrCreateKVStore(store_name)
        codec = default_codec_for(acc_type) if acc_type is not None else PickleCodec()
        return KeyedAggregatingStateFactory(store, namespace, key_group, codec, agg_func)

    def getOrCreateKeyedReducingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec, reduce_func: object
    ) -> KeyedReducingStateFactory:
        store = self.getOrCreateKVStore(store_name)
        return KeyedReducingStateFactory(store, namespace, key_group, value_codec, reduce_func)

    def getOrCreateKeyedReducingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, reduce_func: object, value_type=None
    ) -> KeyedReducingStateFactory:
        store = self.getOrCreateKVStore(store_name)
        codec = default_codec_for(value_type) if value_type is not None else PickleCodec()
        return KeyedReducingStateFactory(store, namespace, key_group, codec, reduce_func)


__all__ = ['WitContext', 'convert_config_to_dict']

