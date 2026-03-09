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

from dataclasses import dataclass
from typing import Generic, List, Optional, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import KEYED_MAP_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
MK = TypeVar("MK")
MV = TypeVar("MV")


@dataclass
class KeyedMapEntry(Generic[MK, MV]):
    key: MK
    value: MV


class KeyedMapStateFactory(Generic[MK, MV]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_group: bytes,
        map_key_codec: Codec[MK],
        map_value_codec: Codec[MV],
    ):
        if store is None:
            raise KvError("keyed map state factory store must not be None")
        if namespace is None:
            raise KvError("keyed map state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed map state factory key_group must not be None")
        if map_key_codec is None or map_value_codec is None:
            raise KvError("keyed map state factory map_key_codec and map_value_codec must not be None")
        ensure_ordered_key_codec(map_key_codec, "keyed map inner")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._map_key_codec = map_key_codec
        self._map_value_codec = map_value_codec

    def new_map(self, key_codec: Codec[K]) -> "KeyedMapState[K, MK, MV]":
        ensure_ordered_key_codec(key_codec, "keyed map")
        return KeyedMapState(
            self._store,
            self._namespace,
            key_codec,
            self._map_key_codec,
            self._map_value_codec,
            self._key_group,
        )


class KeyedMapState(Generic[K, MK, MV]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_codec: Codec[K],
        map_key_codec: Codec[MK],
        map_value_codec: Codec[MV],
        key_group: bytes,
    ):
        if namespace is None:
            raise KvError("keyed map state namespace must not be None")
        if key_group is None:
            raise KvError("keyed map state key_group must not be None")
        if map_key_codec is None or map_value_codec is None:
            raise KvError("keyed map state map_key_codec and map_value_codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._key_codec = key_codec
        self._map_key_codec = map_key_codec
        self._map_value_codec = map_value_codec
        ensure_ordered_key_codec(key_codec, "keyed map")
        ensure_ordered_key_codec(map_key_codec, "keyed map inner")

    def put(self, key: K, map_key: MK, value: MV) -> None:
        ck = ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=self._map_key_codec.encode(map_key),
        )
        self._store.put(ck, self._map_value_codec.encode(value))

    def get(self, key: K, map_key: MK) -> Optional[MV]:
        ck = ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=self._map_key_codec.encode(map_key),
        )
        raw = self._store.get(ck)
        if raw is None:
            return None
        return self._map_value_codec.decode(raw)

    def delete(self, key: K, map_key: MK) -> None:
        ck = ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=self._map_key_codec.encode(map_key),
        )
        self._store.delete(ck)

    def clear(self, key: K) -> None:
        prefix_ck = ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=b"",
        )
        self._store.delete_prefix(prefix_ck)

    def all(self, key: K):
        it = self._store.scan_complex(
            self._key_group,
            self._key_codec.encode(key),
            self._namespace,
        )
        while it.has_next():
            item = it.next()
            if item is None:
                break
            key_bytes, value_bytes = item
            yield self._map_key_codec.decode(key_bytes), self._map_value_codec.decode(value_bytes)

    def len(self, key: K) -> int:
        it = self._store.scan_complex(
            self._key_group,
            self._key_codec.encode(key),
            self._namespace,
        )
        n = 0
        while it.has_next():
            it.next()
            n += 1
        return n

    def entries(self, key: K) -> List[KeyedMapEntry[MK, MV]]:
        return [
            KeyedMapEntry(key=k, value=v)
            for k, v in self.all(key)
        ]

    def range(
        self, key: K, start_inclusive: MK, end_exclusive: MK
    ) -> List[KeyedMapEntry[MK, MV]]:
        key_bytes = self._key_codec.encode(key)
        start_bytes = self._map_key_codec.encode(start_inclusive)
        end_bytes = self._map_key_codec.encode(end_exclusive)
        user_keys = self._store.list_complex(
            self._key_group, key_bytes, self._namespace,
            start_bytes, end_bytes,
        )
        out = []
        for uk in user_keys:
            ck = ComplexKey(
                key_group=self._key_group,
                key=key_bytes,
                namespace=self._namespace,
                user_key=uk,
            )
            raw = self._store.get(ck)
            if raw is None:
                raise KvError("map range key disappeared during scan")
            out.append(KeyedMapEntry(
                key=self._map_key_codec.decode(uk),
                value=self._map_value_codec.decode(raw),
            ))
        return out


__all__ = ["KeyedMapEntry", "KeyedMapState", "KeyedMapStateFactory"]
