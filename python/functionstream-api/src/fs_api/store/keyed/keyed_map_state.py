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
from typing import Any, Generic, Iterator, List, Optional, Tuple, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import ensure_ordered_key_codec

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

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        map_key_codec: Codec[MK],
        map_value_codec: Codec[MV],
    ) -> "KeyedMapStateFactory[MK, MV]":
        """Create a KeyedMapStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, namespace, key_group, map_key_codec, map_value_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[MV],
    ) -> "KeyedMapStateFactory[MK, MV]":
        """Create a KeyedMapStateFactory with default (bytes) map key codec from context and store name."""
        from ..codec import BytesCodec
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, namespace, key_group, BytesCodec(), value_codec)

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

    def all(self, key: K) -> Iterator[Tuple[MK, MV]]:
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


__all__ = ["KeyedMapEntry", "KeyedMapState", "KeyedMapStateFactory"]
