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

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")


class KeyedPriorityQueueStateFactory(Generic[V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[V],
    ):
        if store is None:
            raise KvError("keyed priority queue state factory store must not be None")
        if namespace is None:
            raise KvError("keyed priority queue state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed priority queue state factory key_group must not be None")
        if value_codec is None:
            raise KvError("keyed priority queue state factory value codec must not be None")
        ensure_ordered_key_codec(value_codec, "keyed priority queue value")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._value_codec = value_codec

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[V],
    ) -> "KeyedPriorityQueueStateFactory[V]":
        """Create a KeyedPriorityQueueStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, namespace, key_group, value_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        item_type: Optional[type] = None,
    ) -> "KeyedPriorityQueueStateFactory[V]":
        """Create a KeyedPriorityQueueStateFactory with default codec from context and store name."""
        from ..codec import IntCodec, default_codec_for
        store = ctx.getOrCreateKVStore(store_name)
        codec = default_codec_for(item_type) if item_type is not None else IntCodec()
        return cls(store, namespace, key_group, codec)

    def new_priority_queue(self, key_codec: Codec[K]) -> "KeyedPriorityQueueState[K, V]":
        ensure_ordered_key_codec(key_codec, "keyed priority queue")
        return KeyedPriorityQueueState(
            self._store,
            self._namespace,
            key_codec,
            self._value_codec,
            self._key_group,
        )


class KeyedPriorityQueueState(Generic[K, V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_codec: Codec[K],
        value_codec: Codec[V],
        key_group: bytes,
    ):
        if namespace is None:
            raise KvError("keyed priority queue state namespace must not be None")
        if key_group is None:
            raise KvError("keyed priority queue state key_group must not be None")
        if value_codec is None:
            raise KvError("keyed priority queue state value_codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._key_codec = key_codec
        self._value_codec = value_codec
        ensure_ordered_key_codec(key_codec, "keyed priority queue")
        ensure_ordered_key_codec(value_codec, "keyed priority queue value")

    def _ck(self, key: K, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=user_key,
        )

    def _prefix_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=b"",
        )

    def add(self, key: K, value: V) -> None:
        user_key = self._value_codec.encode(value)
        self._store.put(self._ck(key, user_key), b"")

    def peek(self, key: K) -> Tuple[Optional[V], bool]:
        it = self._store.scan_complex(
            self._key_group,
            self._key_codec.encode(key),
            self._namespace,
        )
        if not it.has_next():
            return (None, False)
        item = it.next()
        if item is None:
            return (None, False)
        user_key, _ = item
        return (self._value_codec.decode(user_key), True)

    def poll(self, key: K) -> Tuple[Optional[V], bool]:
        val, found = self.peek(key)
        if not found or val is None:
            return (val, found)
        self._store.delete(self._ck(key, self._value_codec.encode(val)))
        return (val, True)

    def clear(self, key: K) -> None:
        self._store.delete_prefix(self._prefix_ck(key))

    def all(self, key: K) -> Iterator[V]:
        it = self._store.scan_complex(
            self._key_group,
            self._key_codec.encode(key),
            self._namespace,
        )
        while it.has_next():
            item = it.next()
            if item is None:
                break
            user_key, _ = item
            yield self._value_codec.decode(user_key)


__all__ = ["KeyedPriorityQueueState", "KeyedPriorityQueueStateFactory"]
