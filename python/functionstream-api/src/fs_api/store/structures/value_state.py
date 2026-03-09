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

from typing import Generic, Optional, Tuple, TypeVar

from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

T = TypeVar("T")


class ValueState(Generic[T]):
    def __init__(self, store: KvStore, codec: Codec[T]):
        if store is None:
            raise KvError("value state store must not be None")
        if codec is None:
            raise KvError("value state codec must not be None")
        self._store = store
        self._codec = codec
        self._ck = ComplexKey(
            key_group=b"",
            key=b"",
            namespace=b"",
            user_key=b"",
        )

    def update(self, value: T) -> None:
        self._store.put(self._ck, self._codec.encode(value))

    def value(self) -> Tuple[Optional[T], bool]:
        raw = self._store.get(self._ck)
        if raw is None:
            return (None, False)
        return (self._codec.decode(raw), True)

    def clear(self) -> None:
        self._store.delete(self._ck)


__all__ = ["ValueState"]
