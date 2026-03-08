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

from typing import Generic, Iterator, Optional, Tuple, TypeVar

from ..common import PQ_GROUP, validate_state_name
from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

T = TypeVar("T")


class PriorityQueueState(Generic[T]):
    def __init__(self, store: KvStore, name: str, codec: Codec[T]):
        validate_state_name(name)
        if store is None:
            raise KvError("priority queue store must not be None")
        if codec is None:
            raise KvError("priority queue codec must not be None")
        if not getattr(codec, "supports_ordered_keys", False):
            raise KvError("priority queue value codec must be ordered")
        self._store = store
        self._codec = codec
        state_name = name.strip()
        self._key = state_name.encode("utf-8")
        self._namespace = b"items"

    def _ck(self, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=PQ_GROUP,
            key=self._key,
            namespace=self._namespace,
            user_key=user_key,
        )

    def add(self, value: T) -> None:
        user_key = self._codec.encode(value)
        self._store.put(self._ck(user_key), b"")

    def peek(self) -> Tuple[Optional[T], bool]:
        it = self._store.scan_complex(PQ_GROUP, self._key, self._namespace)
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
            ComplexKey(key_group=PQ_GROUP, key=self._key, namespace=self._namespace, user_key=b"")
        )

    def all(self) -> Iterator[T]:
        it = self._store.scan_complex(PQ_GROUP, self._key, self._namespace)
        while it.has_next():
            item = it.next()
            if item is None:
                break
            user_key, _ = item
            yield self._codec.decode(user_key)


__all__ = ["PriorityQueueState"]
