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

from typing import Generic, Optional, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

from ._keyed_common import KEYED_VALUE_GROUP, ensure_ordered_key_codec

K = TypeVar("K")
V = TypeVar("V")


class KeyedValueStateFactory(Generic[V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_group: bytes,
        value_codec: Codec[V],
    ):
        if store is None:
            raise KvError("keyed value state factory store must not be None")
        if namespace is None:
            raise KvError("keyed value state factory namespace must not be None")
        if key_group is None:
            raise KvError("keyed value state factory key_group must not be None")
        if value_codec is None:
            raise KvError("keyed value state factory value codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._value_codec = value_codec

    def new_value(self, key_codec: Codec[K]) -> "KeyedValueState[K, V]":
        ensure_ordered_key_codec(key_codec, "keyed value")
        return KeyedValueState(
            self._store,
            self._namespace,
            key_codec,
            self._value_codec,
            self._key_group,
        )


class KeyedValueState(Generic[K, V]):
    def __init__(
        self,
        store: KvStore,
        namespace: bytes,
        key_codec: Codec[K],
        value_codec: Codec[V],
        key_group: bytes,
    ):
        if namespace is None:
            raise KvError("keyed value state namespace must not be None")
        if key_group is None:
            raise KvError("keyed value state key_group must not be None")
        if value_codec is None:
            raise KvError("keyed value state value_codec must not be None")
        self._store = store
        self._namespace = namespace
        self._key_group = key_group
        self._key_codec = key_codec
        self._value_codec = value_codec
        ensure_ordered_key_codec(key_codec, "keyed value")

    def _build_ck(self, key: K) -> ComplexKey:
        return ComplexKey(
            key_group=self._key_group,
            key=self._key_codec.encode(key),
            namespace=self._namespace,
            user_key=b"",
        )

    def set(self, key: K, value: V) -> None:
        ck = self._build_ck(key)
        self._store.put(ck, self._value_codec.encode(value))

    def get(self, key: K) -> Optional[V]:
        ck = self._build_ck(key)
        raw = self._store.get(ck)
        if raw is None:
            return None
        return self._value_codec.decode(raw)

    def delete(self, key: K) -> None:
        self._store.delete(self._build_ck(key))


__all__ = ["KeyedValueState", "KeyedValueStateFactory"]
