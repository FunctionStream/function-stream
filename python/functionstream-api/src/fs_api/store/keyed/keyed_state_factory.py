# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import TypeVar

from ..codec import Codec
from ..error import KvError
from ..store import KvStore

from .keyed_aggregating_state import AggregateFunc, KeyedAggregatingState
from .keyed_list_state import KeyedListState
from .keyed_map_state import KeyedMapState
from .keyed_priority_queue_state import KeyedPriorityQueueState
from .keyed_reducing_state import KeyedReducingState, ReduceFunc
from .keyed_value_state import KeyedValueState

K = TypeVar("K")
V = TypeVar("V")
MK = TypeVar("MK")
MV = TypeVar("MV")
T_agg = TypeVar("T_agg")
ACC = TypeVar("ACC")
R = TypeVar("R")


class KeyedStateFactory:
    def __init__(self, store: KvStore, name: str):
        if store is None:
            raise KvError("keyed state factory store must not be None")
        if not isinstance(name, str) or not name.strip():
            raise KvError("keyed state factory name must be non-empty")
        self._store = store
        self._name = name.strip()
        self._kind = None

    def new_keyed_value(self, key_codec: Codec[K], value_codec: Codec[V]) -> KeyedValueState[K, V]:
        self._claim_kind("value")
        return KeyedValueState(self._store, self._name, key_codec, value_codec)

    def new_keyed_map(
        self, key_codec: Codec[K], map_key_codec: Codec[MK], map_value_codec: Codec[MV]
    ) -> KeyedMapState[K, MK, MV]:
        self._claim_kind("map")
        return KeyedMapState(
            self._store, self._name, key_codec, map_key_codec, map_value_codec
        )

    def new_keyed_list(self, key_codec: Codec[K], value_codec: Codec[V]) -> KeyedListState[K, V]:
        self._claim_kind("list")
        return KeyedListState(self._store, self._name, key_codec, value_codec)

    def new_keyed_priority_queue(
        self, key_codec: Codec[K], value_codec: Codec[V]
    ) -> KeyedPriorityQueueState[K, V]:
        self._claim_kind("priority_queue")
        return KeyedPriorityQueueState(self._store, self._name, key_codec, value_codec)

    def new_keyed_aggregating(
        self,
        key_codec: Codec[K],
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
    ) -> KeyedAggregatingState[K, T_agg, ACC, R]:
        self._claim_kind("aggregating")
        return KeyedAggregatingState(
            self._store, self._name, key_codec, acc_codec, agg_func
        )

    def new_keyed_reducing(
        self, key_codec: Codec[K], value_codec: Codec[V], reduce_func: ReduceFunc[V]
    ) -> KeyedReducingState[K, V]:
        self._claim_kind("reducing")
        return KeyedReducingState(self._store, self._name, key_codec, value_codec, reduce_func)

    def _claim_kind(self, kind: str) -> None:
        if self._kind is None:
            self._kind = kind
            return
        if self._kind != kind:
            raise KvError(
                f"keyed state factory '{self._name}' already bound to '{self._kind}', cannot create '{kind}'"
            )


__all__ = ["KeyedStateFactory"]
