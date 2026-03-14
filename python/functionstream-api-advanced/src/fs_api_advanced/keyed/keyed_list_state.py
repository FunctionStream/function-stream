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

import struct
from typing import Any, Generic, List, Optional, TypeVar

from fs_api.store import ComplexKey, KvError, KvStore
from fs_api_advanced.codec import Codec, default_codec_for

V = TypeVar("V")


def _fixed_encoded_size(codec: Codec[Any]) -> tuple[int, bool]:
    """Return (fixed_size, is_fixed). is_fixed is True when encoded_size() > 0."""
    n = codec.encoded_size()
    return (n, n > 0)


class KeyedListStateFactory(Generic[V]):
    """Factory for keyed list state. Create from context with key_group; obtain list per (key, namespace)."""

    def __init__(
        self,
        store: KvStore,
        key_group: bytes,
        value_codec: Codec[V],
    ):
        if store is None:
            raise KvError("keyed list state factory store must not be None")
        if key_group is None:
            raise KvError("keyed list state factory key_group must not be None")
        if value_codec is None:
            raise KvError("keyed list value codec must not be None")
        self._store = store
        self._key_group = key_group
        self._value_codec = value_codec
        self._fixed_size, self._is_fixed = _fixed_encoded_size(value_codec)

    @classmethod
    def from_context(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        value_codec: Codec[V],
    ) -> "KeyedListStateFactory[V]":
        """Create a KeyedListStateFactory from a context and store name (for keyed operators)."""
        store = ctx.getOrCreateKVStore(store_name)
        return cls(store, key_group, value_codec)

    @classmethod
    def from_context_auto_codec(
        cls,
        ctx: Any,
        store_name: str,
        key_group: bytes,
        value_type: Optional[type] = None,
    ) -> "KeyedListStateFactory[V]":
        """Create a KeyedListStateFactory with default value codec from context and store name."""
        store = ctx.getOrCreateKVStore(store_name)
        if value_type is None:
            raise KvError("keyed list state from_context_auto_codec requires value_type")
        codec = default_codec_for(value_type)
        return cls(store, key_group, codec)

    def new_keyed_list(self, key: bytes, namespace: bytes) -> "KeyedListState[V]":
        """Create a KeyedListState for the given key and namespace (e.g. stream key and window namespace)."""
        return KeyedListState(self, key, namespace)


class KeyedListState(Generic[V]):
    """List state for one (key, namespace). Use add/add_all to append, get to read, update to replace, clear to remove."""

    def __init__(self, factory: KeyedListStateFactory[V], key: bytes, namespace: bytes):
        if factory is None:
            raise KvError("keyed list factory must not be None")
        if key is None:
            raise KvError("keyed list key must not be None")
        if namespace is None:
            raise KvError("keyed list namespace must not be None")
        self._factory = factory
        self._key = key
        self._namespace = namespace

    def _complex_key(self) -> ComplexKey:
        return ComplexKey(
            key_group=self._factory._key_group,
            key=self._key,
            namespace=self._namespace,
            user_key=b"",
        )

    def add(self, value: V) -> None:
        payload = self._serialize_one(value)
        self._factory._store.merge(self._complex_key(), payload)

    def add_all(self, values: List[V]) -> None:
        if not values:
            return
        payload = self._serialize_batch(values)
        self._factory._store.merge(self._complex_key(), payload)

    def get(self) -> List[V]:
        raw = self._factory._store.get(self._complex_key())
        if raw is None:
            return []
        return self._deserialize(raw)

    def update(self, values: List[V]) -> None:
        """Replace the list with the given values (clear then put batch, matching Go)."""
        self.clear()
        payload = self._serialize_batch(values)
        self._factory._store.put(self._complex_key(), payload)

    def clear(self) -> None:
        self._factory._store.delete(self._complex_key())

    def _serialize_one(self, value: V) -> bytes:
        if self._factory._is_fixed:
            return self._serialize_one_fixed(value)
        return self._serialize_one_var_len(value)

    def _serialize_batch(self, values: List[V]) -> bytes:
        if self._factory._is_fixed:
            return self._serialize_batch_fixed(values)
        return self._serialize_batch_var_len(values)

    def _deserialize(self, raw: bytes) -> List[V]:
        if self._factory._is_fixed:
            return self._deserialize_fixed(raw)
        return self._deserialize_var_len(raw)

    def _serialize_one_var_len(self, value: V) -> bytes:
        encoded = self._factory._value_codec.encode(value)
        return struct.pack(">I", len(encoded)) + encoded

    def _serialize_batch_var_len(self, values: List[V]) -> bytes:
        parts: List[bytes] = []
        for v in values:
            encoded = self._factory._value_codec.encode(v)
            parts.append(struct.pack(">I", len(encoded)) + encoded)
        return b"".join(parts)

    def _deserialize_var_len(self, raw: bytes) -> List[V]:
        out: List[V] = []
        idx = 0
        while idx < len(raw):
            if len(raw) - idx < 4:
                raise KvError("corrupted keyed list payload: truncated length")
            (item_len,) = struct.unpack(">I", raw[idx : idx + 4])
            idx += 4
            if item_len < 0 or len(raw) - idx < item_len:
                raise KvError("corrupted keyed list payload: invalid element length")
            item_raw = raw[idx : idx + item_len]
            idx += item_len
            out.append(self._factory._value_codec.decode(item_raw))
        return out

    def _serialize_one_fixed(self, value: V) -> bytes:
        fixed = self._factory._fixed_size
        if fixed <= 0:
            raise KvError("fixed-size codec must report positive size")
        encoded = self._factory._value_codec.encode(value)
        if len(encoded) != fixed:
            raise KvError(
                f"fixed-size codec encoded unexpected length: got {len(encoded)}, want {fixed}"
            )
        return encoded

    def _serialize_batch_fixed(self, values: List[V]) -> bytes:
        fixed = self._factory._fixed_size
        if fixed <= 0:
            raise KvError("fixed-size codec must report positive size")
        parts: List[bytes] = []
        for v in values:
            encoded = self._factory._value_codec.encode(v)
            if len(encoded) != fixed:
                raise KvError(
                    f"fixed-size codec encoded unexpected length: got {len(encoded)}, want {fixed}"
                )
            parts.append(encoded)
        return b"".join(parts)

    def _deserialize_fixed(self, raw: bytes) -> List[V]:
        fixed = self._factory._fixed_size
        if fixed <= 0:
            raise KvError("fixed-size codec must report positive size")
        if len(raw) % fixed != 0:
            raise KvError("corrupted keyed list payload: fixed-size data length mismatch")
        out: List[V] = []
        idx = 0
        while idx < len(raw):
            item_raw = raw[idx : idx + fixed]
            idx += fixed
            out.append(self._factory._value_codec.decode(item_raw))
        return out


__all__ = ["KeyedListState", "KeyedListStateFactory"]
