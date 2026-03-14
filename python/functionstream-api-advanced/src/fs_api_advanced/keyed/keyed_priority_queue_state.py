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

from typing import Any, Generic, Iterator, Optional, Tuple, TypeVar

from fs_api.store import ComplexKey, KvError, KvStore
from fs_api_advanced.codec import Codec, default_codec_for

from ._keyed_common import ensure_ordered_key_codec

V = TypeVar("V")


class KeyedPriorityQueueStateFactory(Generic[V]):
    """Factory for keyed priority queue state. Create from context with key_group; obtain queue per (primary_key, namespace)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        value_codec: Codec[V],
    ):
        if store is None:
            raise KvError("keyed priority queue state factory store must not be None")
        if key_group is None:
            raise KvError("keyed priority queue state factory key_group must not be None")
        if value_codec is None:
            raise KvError("keyed priority queue state factory value codec must not be None")
        ensure_ordered_key_codec(value_codec, "keyed priority queue value")
        self._store = store
        self._key_group = key_group
        self._value_codec = value_codec

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        item_codec: Codec[V],
    ) -> "KeyedPriorityQueueStateFactory[V]":
        """Create a KeyedPriorityQueueStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, item_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        item_type: Optional[type] = None,
    ) -> "KeyedPriorityQueueStateFactory[V]":
        """Create a KeyedPriorityQueueStateFactory with default value codec. V must have an ordered default codec."""
        store = ctx.getOrCreateKVStore(store_name)
        if item_type is None:
            raise KvError("keyed priority queue state from_context_auto_codec requires item_type")
        codec = default_codec_for(item_type)
        return cls(store, key_group, codec)

    def new_keyed_priority_queue(
        self, primary_key: bytes, namespace: bytes
    ) -> "KeyedPriorityQueueState[V]":
        """Create a KeyedPriorityQueueState for the given primary key and namespace."""
        if primary_key is None:
            raise KvError("keyed priority queue state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed priority queue state namespace is required")
        return KeyedPriorityQueueState(self, primary_key, namespace)


class KeyedPriorityQueueState(Generic[V]):
    """Priority queue state for one (primary_key, namespace). Elements ordered by encoded user key. add/peek/poll/clear/all."""

    def __init__(
        self,
        factory: KeyedPriorityQueueStateFactory[V],
        primary_key: bytes,
        namespace: bytes,
    ):
        if factory is None:
            raise KvError("keyed priority queue state factory must not be None")
        if primary_key is None:
            raise KvError("keyed priority queue state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed priority queue state namespace must not be None")
        self._factory = factory
        self._primary_key = primary_key
        self._namespace = namespace

    def _complex_key(self, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=self._factory._key_group,
            key=self._primary_key,
            namespace=self._namespace,
            user_key=user_key,
        )

    def _prefix_ck(self) -> ComplexKey:
        return ComplexKey(
            key_group=self._factory._key_group,
            key=self._primary_key,
            namespace=self._namespace,
            user_key=b"",
        )

    def add(self, value: V) -> None:
        """Add an element (stored with encoded value as user key, empty value bytes)."""
        user_key = self._factory._value_codec.encode(value)
        self._factory._store.put(self._complex_key(user_key), b"")

    def peek(self) -> Tuple[Optional[V], bool]:
        """Return (min element, found). found is False when queue is empty."""
        it = self._factory._store.scan_complex(
            self._factory._key_group,
            self._primary_key,
            self._namespace,
        )
        try:
            if not it.has_next():
                return (None, False)
            item = it.next()
            if item is None:
                return (None, False)
            user_key, _ = item
            return (self._factory._value_codec.decode(user_key), True)
        finally:
            if hasattr(it, "close"):
                it.close()

    def poll(self) -> Tuple[Optional[V], bool]:
        """Remove and return (min element, found). Same as peek then delete if found."""
        val, found = self.peek()
        if not found:
            return (val, found)
        user_key = self._factory._value_codec.encode(val)
        self._factory._store.delete(self._complex_key(user_key))
        return (val, True)

    def clear(self) -> None:
        """Remove all elements in this queue (delete by prefix)."""
        self._factory._store.delete_prefix(self._prefix_ck())

    def all(self) -> Iterator[V]:
        """Iterate over all elements in order. Skips entries that fail to decode."""
        it = self._factory._store.scan_complex(
            self._factory._key_group,
            self._primary_key,
            self._namespace,
        )
        try:
            while it.has_next():
                item = it.next()
                if item is None:
                    break
                user_key, _ = item
                try:
                    yield self._factory._value_codec.decode(user_key)
                except Exception:
                    continue
        finally:
            if hasattr(it, "close"):
                it.close()


__all__ = ["KeyedPriorityQueueState", "KeyedPriorityQueueStateFactory"]
