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

from .error import KvError, KvNotFoundError, KvIOError, KvOtherError
from .complexkey import ComplexKey
from .iterator import KvIterator
from .store import KvStore

from .codec import (
    Codec,
    JsonCodec,
    PickleCodec,
    BytesCodec,
    StringCodec,
    BoolCodec,
    Int64Codec,
    Uint64Codec,
    Int32Codec,
    Uint32Codec,
    Float64Codec,
    Float32Codec,
    OrderedInt64Codec,
    OrderedUint64Codec,
    OrderedInt32Codec,
    OrderedUint32Codec,
    OrderedFloat64Codec,
    OrderedFloat32Codec,
)

from .structures import (
    ValueState,
    ListState,
    MapEntry,
    MapState,
    infer_ordered_key_codec,
    create_map_state_auto_key_codec,
    PriorityQueueState,
    AggregateFunc,
    AggregatingState,
    ReduceFunc,
    ReducingState,
)

from .keyed import (
    KeyedStateFactory,
    KeyedValueState,
    KeyedMapState,
    KeyedListState,
    KeyedPriorityQueueState,
    KeyedAggregatingState,
    KeyedReducingState,
)

__all__ = [
    "KvError",
    "KvNotFoundError",
    "KvIOError",
    "KvOtherError",
    "ComplexKey",
    "KvIterator",
    "KvStore",
    "Codec",
    "JsonCodec",
    "PickleCodec",
    "BytesCodec",
    "StringCodec",
    "BoolCodec",
    "Int64Codec",
    "Uint64Codec",
    "Int32Codec",
    "Uint32Codec",
    "Float64Codec",
    "Float32Codec",
    "OrderedInt64Codec",
    "OrderedUint64Codec",
    "OrderedInt32Codec",
    "OrderedUint32Codec",
    "OrderedFloat64Codec",
    "OrderedFloat32Codec",
    "ValueState",
    "MapEntry",
    "MapState",
    "infer_ordered_key_codec",
    "create_map_state_auto_key_codec",
    "ListState",
    "PriorityQueueState",
    "AggregateFunc",
    "AggregatingState",
    "ReduceFunc",
    "ReducingState",
    "KeyedStateFactory",
    "KeyedValueState",
    "KeyedMapState",
    "KeyedListState",
    "KeyedPriorityQueueState",
    "KeyedAggregatingState",
    "KeyedReducingState",
]
