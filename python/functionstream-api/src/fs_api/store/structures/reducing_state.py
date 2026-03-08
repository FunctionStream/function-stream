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

from typing import Callable, Generic, Optional, Tuple, TypeVar

from ..common import REDUCING_PREFIX, validate_state_name
from ..codec import Codec
from ..complexkey import ComplexKey
from ..error import KvError
from ..store import KvStore

V = TypeVar("V")

ReduceFunc = Callable[[V, V], V]


class ReducingState(Generic[V]):
    def __init__(self, store: KvStore, name: str, value_codec: Codec[V], reduce_func: ReduceFunc[V]):
        validate_state_name(name)
        if store is None:
            raise KvError("reducing state store must not be None")
        if value_codec is None or reduce_func is None:
            raise KvError("reducing state value codec and reduce function are required")
        self._store = store
        self._value_codec = value_codec
        self._reduce_func = reduce_func
        state_name = name.strip()
        self._ck = ComplexKey(
            key_group=REDUCING_PREFIX,
            key=state_name.encode("utf-8"),
            namespace=b"data",
            user_key=b"",
        )

    def add(self, value: V) -> None:
        raw = self._store.get(self._ck)
        if raw is None:
            result = value
        else:
            old_value = self._value_codec.decode(raw)
            result = self._reduce_func(old_value, value)
        self._store.put(self._ck, self._value_codec.encode(result))

    def get(self) -> Tuple[Optional[V], bool]:
        raw = self._store.get(self._ck)
        if raw is None:
            return (None, False)
        return (self._value_codec.decode(raw), True)

    def clear(self) -> None:
        self._store.delete(self._ck)


__all__ = ["ReduceFunc", "ReducingState"]
