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
from fs_api_advanced.codec import Codec, PickleCodec, default_codec_for

V = TypeVar("V")

# ReduceFunc(value1, value2) -> result. May raise on error (matches Go's (V, error) return).
ReduceFunc = Callable[[V, V], V]


class KeyedReducingStateFactory(Generic[V]):
    """Factory for keyed reducing state. Create from context with key_group; obtain state per (primary_key, namespace)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ):
        if store is None:
            raise KvError("keyed reducing state factory store must not be None")
        if key_group is None:
            raise KvError("keyed reducing state factory key_group must not be None")
        if value_codec is None or reduce_func is None:
            raise KvError(
                "keyed reducing state factory value_codec and reduce_func must not be None"
            )
        self._store = store
        self._key_group = key_group
        self._value_codec = value_codec
        self._reduce_func = reduce_func

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        value_codec: Codec[V],
        reduce_func: ReduceFunc[V],
    ) -> "KeyedReducingStateFactory[V]":
        """Create a KeyedReducingStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, value_codec, reduce_func)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        reduce_func: ReduceFunc[V],
        value_type: Optional[type] = None,
    ) -> "KeyedReducingStateFactory[V]":
        """Create a KeyedReducingStateFactory with default value codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        codec = default_codec_for(value_type) if value_type is not None else PickleCodec()
        return cls(store, key_group, codec, reduce_func)

    def new_reducing_state(
        self, primary_key: bytes, namespace: bytes
    ) -> "KeyedReducingState[V]":
        """Create a KeyedReducingState for the given primary key and namespace."""
        if primary_key is None:
            raise KvError("keyed reducing state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed reducing state namespace is required")
        return KeyedReducingState(self, primary_key, namespace)


class KeyedReducingState(Generic[V]):
    """Reducing state for one (primary_key, namespace). add(value) merges with reduce_func; get() returns current value; clear() removes."""

    def __init__(
        self,
        factory: KeyedReducingStateFactory[V],
        primary_key: bytes,
        namespace: bytes,
    ):
        if factory is None:
            raise KvError("keyed reducing state factory must not be None")
        if primary_key is None:
            raise KvError("keyed reducing state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed reducing state namespace must not be None")
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

    def add(self, value: V) -> None:
        """Merge value into this state using the factory's reduce function (get current, reduce with value, put)."""
        ck = self._complex_key()
        raw = self._factory._store.get(ck)
        if raw is None:
            result = value
        else:
            old_value = self._factory._value_codec.decode(raw)
            result = self._factory._reduce_func(old_value, value)
        self._factory._store.put(ck, self._factory._value_codec.encode(result))

    def get(self) -> Tuple[Optional[V], bool]:
        """Return (current value, found). found is False when no value has been set."""
        raw = self._factory._store.get(self._complex_key())
        if raw is None:
            return (None, False)
        return (self._factory._value_codec.decode(raw), True)

    def clear(self) -> None:
        """Remove the stored value for this state."""
        self._factory._store.delete(self._complex_key())


__all__ = ["ReduceFunc", "KeyedReducingState", "KeyedReducingStateFactory"]
