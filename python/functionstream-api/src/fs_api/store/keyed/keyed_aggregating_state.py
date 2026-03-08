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

from typing import Generic, Optional, Protocol, Tuple, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..store import KvStore

from ._keyed_common import KEYED_AGGREGATING_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
T_agg = TypeVar("T_agg")
ACC = TypeVar("ACC")
R = TypeVar("R")


class AggregateFunc(Protocol[T_agg, ACC, R]):
    def create_accumulator(self) -> ACC: ...
    def add(self, value: T_agg, accumulator: ACC) -> ACC: ...
    def get_result(self, accumulator: ACC) -> R: ...
    def merge(self, a: ACC, b: ACC) -> ACC: ...


class KeyedAggregatingState(Generic[K, T_agg, ACC, R]):
    def __init__(
        self,
        store: KvStore,
        name: str,
        key_codec: Codec[K],
        acc_codec: Codec[ACC],
        agg_func: AggregateFunc[T_agg, ACC, R],
    ):
        self._store = store
        self._name = name.strip()
        self._key_codec = key_codec
        self._acc_codec = acc_codec
        self._agg_func = agg_func
        ensure_ordered_key_codec(key_codec, "keyed aggregating")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=KEYED_AGGREGATING_GROUP,
            key=self._key_codec.encode(key),
            namespace=self._name.encode("utf-8"),
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


__all__ = ["AggregateFunc", "KeyedAggregatingState"]
