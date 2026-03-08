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
from typing import Generic, List, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import KEYED_LIST_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")


class KeyedListState(Generic[K, V]):
    def __init__(self, store: KvStore, name: str, key_codec: Codec[K], value_codec: Codec[V]):
        self._store = store
        self._name = name.strip()
        self._key_codec = key_codec
        self._value_codec = value_codec
        ensure_ordered_key_codec(key_codec, "keyed list")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=KEYED_LIST_GROUP,
            key=self._key_codec.encode(key),
            namespace=self._name.encode("utf-8"),
            user_key=b"",
        )

    def add(self, key: K, value: V) -> None:
        payload = self._serialize_one(value)
        self._store.merge(self._build_ck(key), payload)

    def add_all(self, key: K, values: List[V]) -> None:
        if not values:
            return
        payload = self._serialize_batch(values)
        self._store.merge(self._build_ck(key), payload)

    def get(self, key: K) -> List[V]:
        raw = self._store.get(self._build_ck(key))
        if raw is None:
            return []
        return self._deserialize(raw)

    def update(self, key: K, values: List[V]) -> None:
        if len(values) == 0:
            self.clear(key)
            return
        self._store.put(self._build_ck(key), self._serialize_batch(values))

    def clear(self, key: K) -> None:
        self._store.delete(self._build_ck(key))

    def _serialize_one(self, value: V) -> bytes:
        encoded = self._value_codec.encode(value)
        return struct.pack(">I", len(encoded)) + encoded

    def _serialize_batch(self, values: List[V]) -> bytes:
        parts = []
        for v in values:
            encoded = self._value_codec.encode(v)
            parts.append(struct.pack(">I", len(encoded)) + encoded)
        return b"".join(parts)

    def _deserialize(self, raw: bytes) -> List[V]:
        out = []
        idx = 0
        while idx < len(raw):
            if len(raw) - idx < 4:
                raise KvError("corrupted keyed list payload: truncated length")
            (item_len,) = struct.unpack(">I", raw[idx : idx + 4])
            idx += 4
            if item_len < 0 or len(raw) - idx < item_len:
                raise KvError("corrupted keyed list payload: invalid element length")
            out.append(self._value_codec.decode(raw[idx : idx + item_len]))
            idx += item_len
        return out


__all__ = ["KeyedListState"]
