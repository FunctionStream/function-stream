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

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")

ReduceFunc = Callable[[V, V], V]


class KeyedReducingStateFactory(Generic[V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ):
        if store is None:
            raise KvError("keyed reducing state factory store must not be None")
        if namespace is None:
            raise KvError("keyed reducing state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed reducing state factory key_group must not be None")
        if value_codec is None or reduce_func is None:
            raise KvError("keyed reducing state factory value_codec and reduce_func must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._value_codec = value_codec
        self._reduce_func = reduce_func

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ) -> "KeyedReducingStateFactory[V]":
        """Create a KeyedReducingStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, namespace, key_group, value_codec, reduce_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        reduce_func: ReduceFunc[V],
        value_type: Optional[type] = None,
    ) -> "KeyedReducingStateFactory[V]":
        """Create a KeyedReducingStateFactory with default value codec from context and store name."""
        from ..codec import PickleCodec, default_codec_for
        store = ctx.getOrCreateKVStore(store_name)
        codec = default_codec_for(value_type) if value_type is not None else PickleCodec()
        return cls(store, namespace, key_group, codec, reduce_func)

    def new_reducing(self, key_codec: Codec[K]) -> "KeyedReducingState[K, V]":
        ensure_ordered_key_codec(key_codec, "keyed reducing")
        return KeyedReducingState(
            self._store,
            self._namespace,
            key_codec,
            self._value_codec,
            self._reduce_func,
            self._key_group,
        )


class KeyedReducingState(Generic[K, V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_codec: Codec[K],
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
        key_group: bytes,
    ):
        if namespace is None:
            raise KvError("keyed reducing state namespace must not be None")
        if key_group is None:
            raise KvError("keyed reducing state key_group must not be None")
        if value_codec is None:
            raise KvError("keyed reducing state value_codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._key_codec = key_codec
        self._value_codec = value_codec
        self._reduce_func = reduce_func
        ensure_ordered_key_codec(key_codec, "keyed reducing")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
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


__all__ = ["ReduceFunc", "KeyedReducingState", "KeyedReducingStateFactory"]
