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

from typing import Any, Callable, Generic, Optional, Tuple, TypeVar

from fs_api.store import ComplexKey, KvError, KvStore
from fs_api_advanced.codec import Codec, PickleCodec

V = TypeVar("V")

ReduceFunc = Callable[[V, V], V]


class ReducingState(Generic[V]):
    def __init__(self, store: KvStore, value_codec: Codec[V], reduce_func: ReduceFunc[V]):
        if store is None:
            raise KvError("reducing state store must not be None")
        if value_codec is None or reduce_func is None:
            raise KvError("reducing state value codec and reduce function are required")
        self._store = store
        self._value_codec = value_codec
        self._reduce_func = reduce_func
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
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ) -> "ReducingState[V]":
        """Create a ReducingState from a context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, value_codec, reduce_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        reduce_func: ReduceFunc[V],
    ) -> "ReducingState[V]":
        """Create a ReducingState with default (pickle) value codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, PickleCodec(), reduce_func)

    def add(self, value: V) -> None:
        raw = self._store.get(self._ck)
        if raw is None:
            result = value
        else:
            old_value = self._value_codec.decode(raw)
            result = self._reduce_func(old_value, value)
        self._store.put(self._ck, self._value_codec.encode(result))

    def get(self) -> Tuple[Optional[V], bool]:
        raw = self._store.get(self._ck)
        if raw is None:
            return (None, False)
        return (self._value_codec.decode(raw), True)

    def clear(self) -> None:
        self._store.delete(self._ck)


__all__ = ["ReduceFunc", "ReducingState"]
