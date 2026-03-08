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

from ..common import validate_state_name
from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

T = TypeVar("T")


class ListState(Generic[T]):
    def __init__(self, store: KvStore, name: str, codec: Codec[T]):
        validate_state_name(name)
        if store is None:
            raise KvError("list state store must not be None")
        if codec is None:
            raise KvError("list state codec must not be None")
        self._store = store
        self._codec = codec
        state_name = name.strip()
        self._ck = ComplexKey(
            key_group=b"",
            key=b"",
            namespace=b"",
            user_key=b"",
        )

    def add(self, value: T) -> None:
        payload = self._serialize_one(value)
        self._store.merge(self._ck, payload)

    def add_all(self, values: List[T]) -> None:
        if not values:
            return
        payload = self._serialize_batch(values)
        self._store.merge(self._ck, payload)

    def get(self) -> List[T]:
        raw = self._store.get(self._ck)
        if raw is None:
            return []
        return self._deserialize(raw)

    def update(self, values: List[T]) -> None:
        if len(values) == 0:
            self.clear()
            return
        payload = self._serialize_batch(values)
        self._store.put(self._ck, payload)

    def clear(self) -> None:
        self._store.delete(self._ck)

    def _serialize_one(self, value: T) -> bytes:
        encoded = self._codec.encode(value)
        return struct.pack(">I", len(encoded)) + encoded

    def _serialize_batch(self, values: List[T]) -> bytes:
        parts: List[bytes] = []
        for v in values:
            encoded = self._codec.encode(v)
            parts.append(struct.pack(">I", len(encoded)) + encoded)
        return b"".join(parts)

    def _deserialize(self, raw: bytes) -> List[T]:
        out: List[T] = []
        idx = 0
        while idx < len(raw):
            if len(raw) - idx < 4:
                raise KvError("corrupted list payload: truncated length")
            (item_len,) = struct.unpack(">I", raw[idx : idx + 4])
            idx += 4
            if item_len < 0 or len(raw) - idx < item_len:
                raise KvError("corrupted list payload: invalid element length")
            item_raw = raw[idx : idx + item_len]
            idx += item_len
            out.append(self._codec.decode(item_raw))
        return out


__all__ = ["ListState"]
