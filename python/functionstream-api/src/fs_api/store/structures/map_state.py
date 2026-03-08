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
from typing import Any, Generator, Generic, List, Optional, Tuple, Type, TypeVar

from ..common import validate_state_name
from ..codec import (
    BoolCodec,
    BytesCodec,
    Codec,
    OrderedFloat64Codec,
    OrderedInt64Codec,
    StringCodec,
)
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

K = TypeVar("K")
V = TypeVar("V")


@dataclass
class MapEntry(Generic[K, V]):
    key: K
    value: V


class MapState(Generic[K, V]):
    def __init__(self, store: KvStore, name: str, key_codec: Codec[K], value_codec: Codec[V]):
        validate_state_name(name)
        if store is None:
            raise KvError("map state store must not be None")
        if key_codec is None or value_codec is None:
            raise KvError("map state codecs must not be None")
        if not getattr(key_codec, "supports_ordered_keys", False):
            raise KvError("map state key codec must support ordered key encoding for range scans")
        self._store = store
        self._key_codec = key_codec
        self._value_codec = value_codec
        self._key_group = b""
        self._key = b""
        self._namespace = b""

    @classmethod
    def with_auto_key_codec(
        cls,
        store: KvStore,
        name: str,
        key_type: Type[K],
        value_codec: Codec[V],
    ) -> "MapState[K, V]":
        key_codec = infer_ordered_key_codec(key_type)
        return cls(store, name, key_codec, value_codec)

    def put(self, key: K, value: V) -> None:
        encoded_key = self._key_codec.encode(key)
        encoded_value = self._value_codec.encode(value)
        self._store.put(self._ck(encoded_key), encoded_value)

    def get(self, key: K) -> Optional[V]:
        encoded_key = self._key_codec.encode(key)
        raw = self._store.get(self._ck(encoded_key))
        if raw is None:
            return None
        return self._value_codec.decode(raw)

    def delete(self, key: K) -> None:
        encoded_key = self._key_codec.encode(key)
        self._store.delete(self._ck(encoded_key))

    def clear(self) -> None:
        self._store.delete_prefix(self._ck(b""))

    def all(self) -> Generator[Tuple[K, V], None, None]:
        it = self._store.scan_complex(self._key_group, self._key, self._namespace)
        while it.has_next():
            item = it.next()
            if item is None:
                break
            key_bytes, value_bytes = item
            yield self._key_codec.decode(key_bytes), self._value_codec.decode(value_bytes)

    def len(self) -> int:
        it = self._store.scan_complex(self._key_group, self._key, self._namespace)
        size = 0
        while it.has_next():
            item = it.next()
            if item is None:
                break
            size += 1
        return size

    def entries(self) -> List[MapEntry[K, V]]:
        it = self._store.scan_complex(self._key_group, self._key, self._namespace)
        out: List[MapEntry[K, V]] = []
        while it.has_next():
            item = it.next()
            if item is None:
                break
            key_bytes, value_bytes = item
            out.append(
                MapEntry(
                    key=self._key_codec.decode(key_bytes),
                    value=self._value_codec.decode(value_bytes),
                )
            )
        return out

    def range(self, start_inclusive: K, end_exclusive: K) -> List[MapEntry[K, V]]:
        start_bytes = self._key_codec.encode(start_inclusive)
        end_bytes = self._key_codec.encode(end_exclusive)
        user_keys = self._store.list_complex(self._key_group, self._key, self._namespace, start_bytes, end_bytes)
        out: List[MapEntry[K, V]] = []
        for user_key in user_keys:
            raw = self._store.get(self._ck(user_key))
            if raw is None:
                raise KvError("map range key disappeared during scan")
            out.append(
                MapEntry(
                    key=self._key_codec.decode(user_key),
                    value=self._value_codec.decode(raw),
                )
            )
        return out

    def _ck(self, user_key: bytes) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key,
            namespace=self._namespace,
            user_key=user_key,
        )


def infer_ordered_key_codec(key_type: Type[Any]) -> Codec[Any]:
    if key_type is str:
        return StringCodec()
    if key_type is bytes:
        return BytesCodec()
    if key_type is bool:
        return BoolCodec()
    if key_type is int:
        return OrderedInt64Codec()
    if key_type is float:
        return OrderedFloat64Codec()
    raise KvError("unsupported map key type for auto codec")


def create_map_state_auto_key_codec(
    store: KvStore,
    name: str,
    key_type: Type[K],
    value_codec: Codec[V],
) -> MapState[K, V]:
    return MapState.with_auto_key_codec(store, name, key_type, value_codec)


__all__ = ["MapEntry", "MapState", "infer_ordered_key_codec", "create_map_state_auto_key_codec"]
