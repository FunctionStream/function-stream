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
fs_api.context

Context: Context object
"""
import abc
from typing import Dict
from .store import (
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


class Context(abc.ABC):
    """Context object"""

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
        """
        Get global configuration Map

        Returns:
            Dict[str, str]: Configuration dictionary
        """

    @abc.abstractmethod
    def getOrCreateValueState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> ValueState:
        pass

    @abc.abstractmethod
    def getOrCreateMapState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> MapState:
        pass

    @abc.abstractmethod
    def getOrCreateListState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> ListState:
        pass

    @abc.abstractmethod
    def getOrCreatePriorityQueueState(self, state_name: str, codec: Codec, store_name: str = "__fssdk_structured_state__") -> PriorityQueueState:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedStateFactory(self, state_name: str, store_name: str = "__fssdk_structured_state__") -> KeyedStateFactory:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedValueState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedValueState:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedMapState(self, state_name: str, key_codec: Codec, map_key_codec: Codec, map_value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedMapState:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedListState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedListState:
        pass

    @abc.abstractmethod
    def getOrCreateKeyedPriorityQueueState(self, state_name: str, key_codec: Codec, value_codec: Codec, store_name: str = "__fssdk_structured_state__") -> KeyedPriorityQueueState:
        pass

__all__ = ['Context']
