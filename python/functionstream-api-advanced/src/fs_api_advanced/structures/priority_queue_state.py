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
from fs_api_advanced.codec import Codec, IntCodec

T = TypeVar("T")


class PriorityQueueState(Generic[T]):
    """State for a priority queue. codec must support ordered key encoding (supports_ordered_keys=True)."""

    def __init__(self, store: KvStore, codec: Codec[T]):
        if store is None:
            raise KvError("priority queue store must not be None")
        if codec is None:
            raise KvError("priority queue codec must not be None")
        if not getattr(codec, "supports_ordered_keys", False):
            raise KvError("priority queue codec must support ordered key encoding")
        self._store = store
        self._codec = codec
        self._key_group = b""
        self._key = b""
        self._namespace = b""

    @classmethod
    def from_context(cls, ctx: Any, store_name: str, codec: Codec[T]) -> "PriorityQueueState[T]":
        """Create a PriorityQueueState from a context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, codec)

    @classmethod
    def from_context_auto_codec(cls, ctx: Any, store_name: str) -> "PriorityQueueState[T]":
        """Create a PriorityQueueState with default (int) codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, IntCodec())

    def _ck(self, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key,
            namespace=self._namespace,
            user_key=user_key,
        )

    def add(self, value: T) -> None:
        user_key = self._codec.encode(value)
        self._store.put(self._ck(user_key), b"")

    def peek(self) -> Tuple[Optional[T], bool]:
        it = self._store.scan_complex(self._key_group, self._key, self._namespace)
        if not it.has_next():
            return (None, False)
        item = it.next()
        if item is None:
            return (None, False)
        user_key, _ = item
        return (self._codec.decode(user_key), True)

    def poll(self) -> Tuple[Optional[T], bool]:
        val, found = self.peek()
        if not found or val is None:
            return (val, found)
        user_key = self._codec.encode(val)
        self._store.delete(self._ck(user_key))
        return (val, True)

    def clear(self) -> None:
        self._store.delete_prefix(
            ComplexKey(key_group=self._key_group, key=self._key, namespace=self._namespace, user_key=b"")
        )

    def all(self) -> Iterator[T]:
        it = self._store.scan_complex(self._key_group, self._key, self._namespace)
        while it.has_next():
            item = it.next()
            if item is None:
                break
            user_key, _ = item
            yield self._codec.decode(user_key)


__all__ = ["PriorityQueueState"]
