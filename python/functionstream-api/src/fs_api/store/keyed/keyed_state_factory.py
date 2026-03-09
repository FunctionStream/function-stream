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
    def __init__(self, store: KvStore, namespace: bytes, key_group: bytes):
        if store is None:
            raise KvError("keyed state factory store must not be None")
        if namespace is None:
            raise KvError("keyed state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed state factory key_group must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._kind = None

    def new_keyed_value(self, key_codec: Codec[K], value_codec: Codec[V]) -> KeyedValueState[K, V]:
        self._claim_kind("value")
        if value_codec is None:
            raise KvError("keyed value state value_codec must not be None")
        return KeyedValueState(self._store, self._namespace, key_codec, value_codec, self._key_group)

    def new_keyed_map(
        self, key_codec: Codec[K], map_key_codec: Codec[MK], map_value_codec: Codec[MV]
    ) -> KeyedMapState[K, MK, MV]:
        self._claim_kind("map")
        if map_key_codec is None or map_value_codec is None:
            raise KvError("keyed map state map_key_codec and map_value_codec must not be None")
        return KeyedMapState(
            self._store, self._namespace, key_codec, map_key_codec, map_value_codec, self._key_group
        )

    def new_keyed_list(self, key_codec: Codec[K], value_codec: Codec[V]) -> KeyedListState[K, V]:
        self._claim_kind("list")
        if value_codec is None:
            raise KvError("keyed list state value_codec must not be None")
        return KeyedListState(self._store, self._namespace, key_codec, value_codec, self._key_group)

    def new_keyed_priority_queue(
        self, key_codec: Codec[K], value_codec: Codec[V]
    ) -> KeyedPriorityQueueState[K, V]:
        self._claim_kind("priority_queue")
        if value_codec is None:
            raise KvError("keyed priority queue state value_codec must not be None")
        return KeyedPriorityQueueState(
            self._store, self._namespace, key_codec, value_codec, self._key_group
        )

    def new_keyed_aggregating(
        self,
        key_codec: Codec[K],
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
    ) -> KeyedAggregatingState[K, T_agg, ACC, R]:
        self._claim_kind("aggregating")
        if acc_codec is None or agg_func is None:
            raise KvError("keyed aggregating state acc_codec and agg_func must not be None")
        return KeyedAggregatingState(
            self._store, self._namespace, key_codec, acc_codec, agg_func, self._key_group
        )

    def new_keyed_reducing(
        self, key_codec: Codec[K], value_codec: Codec[V], reduce_func: ReduceFunc[V]
    ) -> KeyedReducingState[K, V]:
        self._claim_kind("reducing")
        if value_codec is None or reduce_func is None:
            raise KvError("keyed reducing state value_codec and reduce_func must not be None")
        return KeyedReducingState(
            self._store, self._namespace, key_codec, value_codec, reduce_func, self._key_group
        )

    def _claim_kind(self, kind: str) -> None:
        if self._kind is None:
            self._kind = kind
            return
        if self._kind != kind:
            raise KvError(
                f"keyed state factory already bound to '{self._kind}', cannot create '{kind}'"
            )


__all__ = ["KeyedStateFactory"]
