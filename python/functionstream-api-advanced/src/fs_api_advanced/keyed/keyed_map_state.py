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

MK = TypeVar("MK")
MV = TypeVar("MV")


class KeyedMapStateFactory(Generic[MK, MV]):
    """Factory for keyed map state. Create from context with key_group; obtain map per (primary_key, map_name)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        map_key_codec: Codec[MK],
        map_value_codec: Codec[MV],
    ):
        if store is None:
            raise KvError("keyed map state factory store must not be None")
        if key_group is None:
            raise KvError("keyed map state factory key_group must not be None")
        if map_key_codec is None or map_value_codec is None:
            raise KvError(
                "keyed map state factory map_key_codec and map_value_codec must not be None"
            )
        ensure_ordered_key_codec(map_key_codec, "keyed map")
        self._store = store
        self._key_group = key_group
        self._map_key_codec = map_key_codec
        self._map_value_codec = map_value_codec

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        map_key_codec: Codec[MK],
        map_value_codec: Codec[MV],
    ) -> "KeyedMapStateFactory[MK, MV]":
        """Create a KeyedMapStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, map_key_codec, map_value_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        map_key_type: Optional[type] = None,
        map_value_type: Optional[type] = None,
    ) -> "KeyedMapStateFactory[MK, MV]":
        """Create a KeyedMapStateFactory with default codecs for MK and MV. Map key type must have an ordered default codec."""
        store = ctx.getOrCreateKVStore(store_name)
        if map_key_type is None:
            raise KvError("keyed map state from_context_auto_codec requires map_key_type")
        if map_value_type is None:
            raise KvError("keyed map state from_context_auto_codec requires map_value_type")
        map_key_codec = default_codec_for(map_key_type)
        map_value_codec = default_codec_for(map_value_type)
        return cls(store, key_group, map_key_codec, map_value_codec)

    def new_keyed_map(
        self, primary_key: bytes, map_name: str
    ) -> "KeyedMapState[MK, MV]":
        """Create a KeyedMapState for the given primary key and map name (map_name becomes namespace)."""
        if primary_key is None:
            raise KvError("keyed map state primary_key must not be None")
        if not map_name:
            raise KvError("keyed map state map_name is required")
        namespace = map_name.encode("utf-8")
        return KeyedMapState(self, primary_key, namespace)


class KeyedMapState(Generic[MK, MV]):
    """Map state for one (primary_key, namespace). put/get/delete by map_key; clear() removes all entries; all() iterates (mk, mv)."""

    def __init__(
        self,
        factory: KeyedMapStateFactory[MK, MV],
        primary_key: bytes,
        namespace: bytes,
    ):
        if factory is None:
            raise KvError("keyed map state factory must not be None")
        if primary_key is None:
            raise KvError("keyed map state primary_key must not be None")
        if namespace is None:
            raise KvError("keyed map state namespace must not be None")
        self._factory = factory
        self._primary_key = primary_key
        self._namespace = namespace

    def _complex_key(self, map_key: MK) -> ComplexKey:
        encoded = self._factory._map_key_codec.encode(map_key)
        return ComplexKey(
            key_group=self._factory._key_group,
            key=self._primary_key,
            namespace=self._namespace,
            user_key=encoded,
        )

    def put(self, map_key: MK, value: MV) -> None:
        ck = self._complex_key(map_key)
        self._factory._store.put(ck, self._factory._map_value_codec.encode(value))

    def get(self, map_key: MK) -> Tuple[Optional[MV], bool]:
        """Return (value, found). found is False when the key is missing."""
        ck = self._complex_key(map_key)
        raw = self._factory._store.get(ck)
        if raw is None:
            return (None, False)
        return (self._factory._map_value_codec.decode(raw), True)

    def delete(self, map_key: MK) -> None:
        ck = self._complex_key(map_key)
        self._factory._store.delete(ck)

    def clear(self) -> None:
        """Remove all entries in this map (delete by prefix)."""
        prefix_ck = ComplexKey(
            key_group=self._factory._key_group,
            key=self._primary_key,
            namespace=self._namespace,
            user_key=b"",
        )
        self._factory._store.delete_prefix(prefix_ck)

    def all(self) -> Iterator[Tuple[MK, MV]]:
        """Iterate over all (map_key, value) pairs in this map. Skips entries that fail to decode."""
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
                key_raw, value_raw = item
                try:
                    k = self._factory._map_key_codec.decode(key_raw)
                except Exception:
                    continue
                try:
                    v = self._factory._map_value_codec.decode(value_raw)
                except Exception:
                    continue
                yield (k, v)
        finally:
            if hasattr(it, "close"):
                it.close()


__all__ = ["KeyedMapState", "KeyedMapStateFactory"]
