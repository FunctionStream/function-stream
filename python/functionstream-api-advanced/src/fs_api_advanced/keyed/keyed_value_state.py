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

from typing import Any, Generic, Optional, Tuple, TypeVar

from fs_api.store import ComplexKey, KvError, KvStore
from fs_api_advanced.codec import Codec, default_codec_for

V = TypeVar("V")


class KeyedValueStateFactory(Generic[V]):
    """Factory for keyed value state. Create from context with key_group; obtain state via new_keyed_value(primary_key, namespace)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        value_codec: Codec[V],
    ):
        if store is None:
            raise KvError("keyed value state factory store must not be None")
        if key_group is None:
            raise KvError("keyed value state factory key_group must not be None")
        if value_codec is None:
            raise KvError("keyed value state factory value codec must not be None")
        self._store = store
        self._key_group = key_group
        self._value_codec = value_codec

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        value_codec: Codec[V],
    ) -> "KeyedValueStateFactory[V]":
        """Create a KeyedValueStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, value_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        value_type: Optional[type] = None,
    ) -> "KeyedValueStateFactory[V]":
        """Create a KeyedValueStateFactory with default value codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        if value_type is None:
            raise KvError("keyed value state from_context_auto_codec requires value_type")
        codec = default_codec_for(value_type)
        return cls(store, key_group, codec)

    def new_keyed_value(
        self, primary_key: bytes, namespace: bytes
    ) -> "KeyedValueState[V]":
        """Create a KeyedValueState for the given primary key and namespace."""
        if primary_key is None:
            raise KvError("keyed value state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed value state namespace is required")
        return KeyedValueState(self, primary_key, namespace)


class KeyedValueState(Generic[V]):
    """Value state for one (primary_key, namespace). update(value), value() -> (value, found), clear()."""

    def __init__(
        self,
        factory: KeyedValueStateFactory[V],
        primary_key: bytes,
        namespace: bytes,
    ):
        if factory is None:
            raise KvError("keyed value state factory must not be None")
        if primary_key is None:
            raise KvError("keyed value state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed value state namespace must not be None")
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

    def update(self, value: V) -> None:
        """Set the value for this state."""
        ck = self._complex_key()
        self._factory._store.put(ck, self._factory._value_codec.encode(value))

    def value(self) -> Tuple[Optional[V], bool]:
        """Return (current value, found). found is False when no value has been set."""
        raw = self._factory._store.get(self._complex_key())
        if raw is None:
            return (None, False)
        return (self._factory._value_codec.decode(raw), True)

    def clear(self) -> None:
        """Remove the stored value for this state."""
        self._factory._store.delete(self._complex_key())


__all__ = ["KeyedValueState", "KeyedValueStateFactory"]
