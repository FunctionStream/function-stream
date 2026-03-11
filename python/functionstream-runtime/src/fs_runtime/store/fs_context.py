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
        return ValueState.from_context(self, store_name, codec)

    def getOrCreateValueStateAutoCodec(self, store_name: str) -> ValueState:
        return ValueState.from_context_auto_codec(self, store_name)

    def getOrCreateMapState(self, store_name: str, key_codec: Codec, value_codec: Codec) -> MapState:
        return MapState.from_context(self, store_name, key_codec, value_codec)

    def getOrCreateMapStateAutoKeyCodec(self, store_name: str, value_codec: Codec) -> MapState:
        return MapState.from_context_auto_key_codec(self, store_name, value_codec)

    def getOrCreateListState(self, store_name: str, codec: Codec) -> ListState:
        return ListState.from_context(self, store_name, codec)

    def getOrCreateListStateAutoCodec(self, store_name: str) -> ListState:
        return ListState.from_context_auto_codec(self, store_name)

    def getOrCreatePriorityQueueState(self, store_name: str, codec: Codec) -> PriorityQueueState:
        return PriorityQueueState.from_context(self, store_name, codec)

    def getOrCreatePriorityQueueStateAutoCodec(self, store_name: str) -> PriorityQueueState:
        return PriorityQueueState.from_context_auto_codec(self, store_name)

    def getOrCreateAggregatingState(
        self, store_name: str, acc_codec: Codec, agg_func: object
    ) -> AggregatingState:
        return AggregatingState.from_context(self, store_name, acc_codec, agg_func)

    def getOrCreateAggregatingStateAutoCodec(
        self, store_name: str, agg_func: object
    ) -> AggregatingState:
        return AggregatingState.from_context_auto_codec(self, store_name, agg_func)

    def getOrCreateReducingState(
        self, store_name: str, value_codec: Codec, reduce_func: object
    ) -> ReducingState:
        return ReducingState.from_context(self, store_name, value_codec, reduce_func)

    def getOrCreateReducingStateAutoCodec(
        self, store_name: str, reduce_func: object
    ) -> ReducingState:
        return ReducingState.from_context_auto_codec(self, store_name, reduce_func)

    def getOrCreateKeyedListStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedListStateFactory:
        return KeyedListStateFactory.from_context(self, store_name, namespace, key_group, value_codec)

    def getOrCreateKeyedListStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type=None
    ) -> KeyedListStateFactory:
        return KeyedListStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, value_type)

    def getOrCreateKeyedValueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedValueStateFactory:
        return KeyedValueStateFactory.from_context(self, store_name, namespace, key_group, value_codec)

    def getOrCreateKeyedValueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type=None
    ) -> KeyedValueStateFactory:
        return KeyedValueStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, value_type)

    def getOrCreateKeyedMapStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, key_codec: Codec, value_codec: Codec
    ) -> KeyedMapStateFactory:
        return KeyedMapStateFactory.from_context(self, store_name, namespace, key_group, key_codec, value_codec)

    def getOrCreateKeyedMapStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedMapStateFactory:
        return KeyedMapStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, value_codec)

    def getOrCreateKeyedPriorityQueueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, item_codec: Codec
    ) -> KeyedPriorityQueueStateFactory:
        return KeyedPriorityQueueStateFactory.from_context(self, store_name, namespace, key_group, item_codec)

    def getOrCreateKeyedPriorityQueueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, item_type=None
    ) -> KeyedPriorityQueueStateFactory:
        return KeyedPriorityQueueStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, item_type)

    def getOrCreateKeyedAggregatingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, acc_codec: Codec, agg_func: object
    ) -> KeyedAggregatingStateFactory:
        return KeyedAggregatingStateFactory.from_context(self, store_name, namespace, key_group, acc_codec, agg_func)

    def getOrCreateKeyedAggregatingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, agg_func: object, acc_type=None
    ) -> KeyedAggregatingStateFactory:
        return KeyedAggregatingStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, agg_func, acc_type)

    def getOrCreateKeyedReducingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec, reduce_func: object
    ) -> KeyedReducingStateFactory:
        return KeyedReducingStateFactory.from_context(self, store_name, namespace, key_group, value_codec, reduce_func)

    def getOrCreateKeyedReducingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, reduce_func: object, value_type=None
    ) -> KeyedReducingStateFactory:
        return KeyedReducingStateFactory.from_context_auto_codec(self, store_name, namespace, key_group, reduce_func, value_type)


__all__ = ['WitContext', 'convert_config_to_dict']

