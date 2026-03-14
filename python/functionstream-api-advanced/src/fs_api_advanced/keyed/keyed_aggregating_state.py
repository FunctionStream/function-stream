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
from fs_api_advanced.codec import Codec, default_codec_for

T_agg = TypeVar("T_agg")
ACC = TypeVar("ACC")
R = TypeVar("R")


class AggregateFunc(Protocol[T_agg, ACC, R]):
    def create_accumulator(self) -> ACC: ...
    def add(self, value: T_agg, accumulator: ACC) -> ACC: ...
    def get_result(self, accumulator: ACC) -> R: ...
    def merge(self, a: ACC, b: ACC) -> ACC: ...


class KeyedAggregatingStateFactory(Generic[T_agg, ACC, R]):
    """Factory for keyed aggregating state. Create from context with key_group; obtain state per (primary_key, state_name)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
    ):
        if store is None:
            raise KvError("keyed aggregating state factory store must not be None")
        if key_group is None:
            raise KvError("keyed aggregating state factory key_group must not be None")
        if acc_codec is None:
            raise KvError("keyed aggregating state factory acc_codec must not be None")
        if agg_func is None:
            raise KvError("keyed aggregating state factory agg_func must not be None")
        self._store = store
        self._key_group = key_group
        self._acc_codec = acc_codec
        self._agg_func = agg_func

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
    ) -> "KeyedAggregatingStateFactory[T_agg, ACC, R]":
        """Create a KeyedAggregatingStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, acc_codec, agg_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        agg_func: AggregateFunc[T_agg, ACC, R],
        acc_type: Optional[type] = None,
    ) -> "KeyedAggregatingStateFactory[T_agg, ACC, R]":
        """Create a KeyedAggregatingStateFactory with default accumulator codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        if acc_type is None:
            raise KvError("keyed aggregating state from_context_auto_codec requires acc_type")
        codec = default_codec_for(acc_type)
        return cls(store, key_group, codec, agg_func)

    def new_aggregating_state(
        self, primary_key: bytes, state_name: str = ""
    ) -> "KeyedAggregatingState[T_agg, ACC, R]":
        """Create a KeyedAggregatingState for the given primary key and state name (state_name becomes namespace bytes)."""
        if primary_key is None:
            raise KvError("keyed aggregating state primary_key must not be None")
        namespace = state_name.encode("utf-8") if state_name else b""
        return KeyedAggregatingState(self, primary_key, namespace)


class KeyedAggregatingState(Generic[T_agg, ACC, R]):
    """Aggregating state for one (primary_key, namespace). Use add(value) to merge, get() to read result, clear() to remove."""

    def __init__(
        self,
        factory: KeyedAggregatingStateFactory[T_agg, ACC, R],
        primary_key: bytes,
        namespace: bytes,
    ):
        if factory is None:
            raise KvError("keyed aggregating state factory must not be None")
        if primary_key is None:
            raise KvError("keyed aggregating state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed aggregating state namespace must not be None")
        self._factory = factory
        self._primary_key = primary_key
        self._namespace = namespace

    def _complex_key(self) -> ComplexKey:
        return ComplexKey(
            key_group=self._factory._key_group,
            key=self._primary_key,
            namespace=self._namespace,
            user_key=b"",
        )

    def add(self, value: T_agg) -> None:
        """Add a value into this state's accumulator (get or create acc, add, put back)."""
        ck = self._complex_key()
        raw = self._factory._store.get(ck)
        if raw is None:
            acc = self._factory._agg_func.create_accumulator()
        else:
            acc = self._factory._acc_codec.decode(raw)
        new_acc = self._factory._agg_func.add(value, acc)
        self._factory._store.put(ck, self._factory._acc_codec.encode(new_acc))

    def get(self) -> Tuple[Optional[R], bool]:
        """Return (result, found). found is False when no accumulator exists."""
        ck = self._complex_key()
        raw = self._factory._store.get(ck)
        if raw is None:
            return (None, False)
        acc = self._factory._acc_codec.decode(raw)
        return (self._factory._agg_func.get_result(acc), True)

    def clear(self) -> None:
        """Remove this state's accumulator."""
        self._factory._store.delete(self._complex_key())


__all__ = ["AggregateFunc", "KeyedAggregatingState", "KeyedAggregatingStateFactory"]
