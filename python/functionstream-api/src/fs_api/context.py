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

import abc
from typing import Dict, Optional, Type

from .store import (
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


class Context(abc.ABC):
    @abc.abstractmethod
    def emit(self, data: bytes, channel: int = 0):
        pass

    @abc.abstractmethod
    def emit_watermark(self, watermark: int, channel: int = 0):
        pass

    @abc.abstractmethod
    def getOrCreateKVStore(self, name: str) -> KvStore:
        pass

    @abc.abstractmethod
    def getConfig(self) -> Dict[str, str]:
        pass

    @abc.abstractmethod
    def getOrCreateValueState(self, store_name: str, codec: Codec) -> ValueState:
        pass

    @abc.abstractmethod
    def getOrCreateValueStateAutoCodec(self, store_name: str) -> ValueState:
        pass

    @abc.abstractmethod
    def getOrCreateMapState(self, store_name: str, key_codec: Codec, value_codec: Codec) -> MapState:
        pass

    @abc.abstractmethod
    def getOrCreateMapStateAutoKeyCodec(self, store_name: str, value_codec: Codec) -> MapState:
        pass

    @abc.abstractmethod
    def getOrCreateListState(self, store_name: str, codec: Codec) -> ListState:
        pass

    @abc.abstractmethod
    def getOrCreateListStateAutoCodec(self, store_name: str) -> ListState:
        pass

    @abc.abstractmethod
    def getOrCreatePriorityQueueState(self, store_name: str, codec: Codec) -> PriorityQueueState:
        pass

    @abc.abstractmethod
    def getOrCreatePriorityQueueStateAutoCodec(self, store_name: str) -> PriorityQueueState:
        pass

    @abc.abstractmethod
    def getOrCreateAggregatingState(
        self, store_name: str, acc_codec: Codec, agg_func: object
    ) -> AggregatingState:
        pass

    @abc.abstractmethod
    def getOrCreateAggregatingStateAutoCodec(
        self, store_name: str, agg_func: object
    ) -> AggregatingState:
        pass

    @abc.abstractmethod
    def getOrCreateReducingState(
        self, store_name: str, value_codec: Codec, reduce_func: object
    ) -> ReducingState:
        pass

    @abc.abstractmethod
    def getOrCreateReducingStateAutoCodec(
        self, store_name: str, reduce_func: object
    ) -> ReducingState:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedListStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedListStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedListStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type: Optional[Type] = None
    ) -> KeyedListStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedValueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedValueStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedValueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_type: Optional[Type] = None
    ) -> KeyedValueStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedMapStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, key_codec: Codec, value_codec: Codec
    ) -> KeyedMapStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedMapStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec
    ) -> KeyedMapStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedPriorityQueueStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, item_codec: Codec
    ) -> KeyedPriorityQueueStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedPriorityQueueStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, item_type: Optional[Type] = None
    ) -> KeyedPriorityQueueStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedAggregatingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, acc_codec: Codec, agg_func: object
    ) -> KeyedAggregatingStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedAggregatingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, agg_func: object, acc_type: Optional[Type] = None
    ) -> KeyedAggregatingStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedReducingStateFactory(
        self, store_name: str, namespace: bytes, key_group: bytes, value_codec: Codec, reduce_func: object
    ) -> KeyedReducingStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedReducingStateFactoryAutoCodec(
        self, store_name: str, namespace: bytes, key_group: bytes, reduce_func: object, value_type: Optional[Type] = None
    ) -> KeyedReducingStateFactory:
        pass


__all__ = ["Context"]
