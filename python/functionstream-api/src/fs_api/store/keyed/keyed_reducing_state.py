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

from typing import Callable, Generic, Optional, Tuple, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..store import KvStore

from ._keyed_common import KEYED_REDUCING_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")

ReduceFunc = Callable[[V, V], V]


class KeyedReducingState(Generic[K, V]):
    def __init__(
        self,
        store: KvStore,
        name: str,
        key_codec: Codec[K],
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ):
        self._store = store
        self._name = name.strip()
        self._key_codec = key_codec
        self._value_codec = value_codec
        self._reduce_func = reduce_func
        ensure_ordered_key_codec(key_codec, "keyed reducing")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=KEYED_REDUCING_GROUP,
            key=self._key_codec.encode(key),
            namespace=self._name.encode("utf-8"),
            user_key=b"",
        )

    def add(self, key: K, value: V) -> None:
        ck = self._build_ck(key)
        raw = self._store.get(ck)
        if raw is None:
            result = value
        else:
            result = self._reduce_func(self._value_codec.decode(raw), value)
        self._store.put(ck, self._value_codec.encode(result))

    def get(self, key: K) -> Tuple[Optional[V], bool]:
        raw = self._store.get(self._build_ck(key))
        if raw is None:
            return (None, False)
        return (self._value_codec.decode(raw), True)

    def clear(self, key: K) -> None:
        self._store.delete(self._build_ck(key))


__all__ = ["ReduceFunc", "KeyedReducingState"]
