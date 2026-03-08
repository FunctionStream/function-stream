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

from ..codec import Codec
from ..complexkey import ComplexKey
from ..store import KvStore

from ._keyed_common import KEYED_PQ_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")


class KeyedPriorityQueueState(Generic[K, V]):
    def __init__(self, store: KvStore, name: str, key_codec: Codec[K], value_codec: Codec[V]):
        self._store = store
        self._name = name.strip()
        self._key_codec = key_codec
        self._value_codec = value_codec
        ensure_ordered_key_codec(key_codec, "keyed priority queue")
        ensure_ordered_key_codec(value_codec, "keyed priority queue value")

    def _ck(self, key: K, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=KEYED_PQ_GROUP,
            key=self._key_codec.encode(key),
            namespace=self._name.encode("utf-8"),
            user_key=user_key,
        )

    def _prefix_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=KEYED_PQ_GROUP,
            key=self._key_codec.encode(key),
            namespace=self._name.encode("utf-8"),
            user_key=b"",
        )

    def add(self, key: K, value: V) -> None:
        user_key = self._value_codec.encode(value)
        self._store.put(self._ck(key, user_key), b"")

    def peek(self, key: K) -> Tuple[Optional[V], bool]:
        it = self._store.scan_complex(
            KEYED_PQ_GROUP,
            self._key_codec.encode(key),
            self._name.encode("utf-8"),
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
            KEYED_PQ_GROUP,
            self._key_codec.encode(key),
            self._name.encode("utf-8"),
        )
        while it.has_next():
            item = it.next()
            if item is None:
                break
            user_key, _ = item
            yield self._value_codec.decode(user_key)


__all__ = ["KeyedPriorityQueueState"]
