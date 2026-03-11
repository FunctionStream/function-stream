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

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

T = TypeVar("T")
ACC = TypeVar("ACC")
R = TypeVar("R")


class AggregateFunc(Protocol[T, ACC, R]):
    def create_accumulator(self) -> ACC:
        ...

    def add(self, value: T, accumulator: ACC) -> ACC:
        ...

    def get_result(self, accumulator: ACC) -> R:
        ...

    def merge(self, a: ACC, b: ACC) -> ACC:
        ...


class AggregatingState(Generic[T, ACC, R]):
    def __init__(
        self,
        store: KvStore,
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T, ACC, R],
    ):
        if store is None:
            raise KvError("aggregating state store must not be None")
        if acc_codec is None:
            raise KvError("aggregating state acc codec must not be None")
        if agg_func is None:
            raise KvError("aggregating state agg func must not be None")
        self._store = store
        self._acc_codec = acc_codec
        self._agg_func = agg_func
        self._ck = ComplexKey(
            key_group=b"",
            key=b"",
            namespace=b"",
            user_key=b"",
        )

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T, ACC, R],
    ) -> "AggregatingState[T, ACC, R]":
        """Create an AggregatingState from a context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, acc_codec, agg_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        agg_func: AggregateFunc[T, ACC, R],
    ) -> "AggregatingState[T, ACC, R]":
        """Create an AggregatingState with default (pickle) accumulator codec from context and store name."""
        from ..codec import PickleCodec
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, PickleCodec(), agg_func)

    def add(self, value: T) -> None:
        raw = self._store.get(self._ck)
        if raw is None:
            acc = self._agg_func.create_accumulator()
        else:
            acc = self._acc_codec.decode(raw)
        new_acc = self._agg_func.add(value, acc)
        self._store.put(self._ck, self._acc_codec.encode(new_acc))

    def get(self) -> Tuple[Optional[R], bool]:
        raw = self._store.get(self._ck)
        if raw is None:
            return (None, False)
        acc = self._acc_codec.decode(raw)
        return (self._agg_func.get_result(acc), True)

    def clear(self) -> None:
        self._store.delete(self._ck)


__all__ = ["AggregateFunc", "AggregatingState"]
