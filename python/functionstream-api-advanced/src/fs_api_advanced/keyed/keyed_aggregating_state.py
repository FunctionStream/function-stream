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

from typing import Any, Generic, Optional, Protocol, Tuple, TypeVar

from fs_api.store import ComplexKey, KvError, KvStore
from fs_api_advanced.codec import Codec, PickleCodec, default_codec_for

from ._keyed_common import ensure_ordered_key_codec

K = TypeVar("K")
T_agg = TypeVar("T_agg")
ACC = TypeVar("ACC")
R = TypeVar("R")


class AggregateFunc(Protocol[T_agg, ACC, R]):
    def create_accumulator(self) -> ACC: ...
    def add(self, value: T_agg, accumulator: ACC) -> ACC: ...
    def get_result(self, accumulator: ACC) -> R: ...
    def merge(self, a: ACC, b: ACC) -> ACC: ...


class KeyedAggregatingStateFactory(Generic[T_agg, ACC, R]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_group: bytes,
        acc_codec: Codec[ACC],
        agg_func: "AggregateFunc[T_agg, ACC, R]",
    ):
        if store is None:
            raise KvError("keyed aggregating state factory store must not be None")
        if namespace is None:
            raise KvError("keyed aggregating state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed aggregating state factory key_group must not be None")
        if acc_codec is None or agg_func is None:
            raise KvError("keyed aggregating state factory acc_codec and agg_func must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._acc_codec = acc_codec
        self._agg_func = agg_func

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        acc_codec: Codec[ACC],
        agg_func: "AggregateFunc[T_agg, ACC, R]",
    ) -> "KeyedAggregatingStateFactory[T_agg, ACC, R]":
        """Create a KeyedAggregatingStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, namespace, key_group, acc_codec, agg_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        agg_func: "AggregateFunc[T_agg, ACC, R]",
        acc_type: Optional[type] = None,
    ) -> "KeyedAggregatingStateFactory[T_agg, ACC, R]":
        """Create a KeyedAggregatingStateFactory with default accumulator codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        codec = default_codec_for(acc_type) if acc_type is not None else PickleCodec()
        return cls(store, namespace, key_group, codec, agg_func)

    def new_aggregating(self, key_codec: Codec[K]) -> "KeyedAggregatingState[K, T_agg, ACC, R]":
        ensure_ordered_key_codec(key_codec, "keyed aggregating")
        return KeyedAggregatingState(
            self._store,
            self._namespace,
            key_codec,
            self._acc_codec,
            self._agg_func,
            self._key_group,
        )


class KeyedAggregatingState(Generic[K, T_agg, ACC, R]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_codec: Codec[K],
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
        key_group: bytes,
    ):
        if namespace is None:
            raise KvError("keyed aggregating state namespace must not be None")
        if key_group is None:
            raise KvError("keyed aggregating state key_group must not be None")
        if acc_codec is None:
            raise KvError("keyed aggregating state acc_codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._key_codec = key_codec
        self._acc_codec = acc_codec
        self._agg_func = agg_func
        ensure_ordered_key_codec(key_codec, "keyed aggregating")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=b"",
        )

    def add(self, key: K, value: T_agg) -> None:
        ck = self._build_ck(key)
        raw = self._store.get(ck)
        if raw is None:
            acc = self._agg_func.create_accumulator()
        else:
            acc = self._acc_codec.decode(raw)
        new_acc = self._agg_func.add(value, acc)
        self._store.put(ck, self._acc_codec.encode(new_acc))

    def get(self, key: K) -> Tuple[Optional[R], bool]:
        ck = self._build_ck(key)
        raw = self._store.get(ck)
        if raw is None:
            return (None, False)
        acc = self._acc_codec.decode(raw)
        return (self._agg_func.get_result(acc), True)

    def clear(self, key: K) -> None:
        self._store.delete(self._build_ck(key))


__all__ = ["AggregateFunc", "KeyedAggregatingState", "KeyedAggregatingStateFactory"]
