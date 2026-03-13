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

"""
Function Stream Advanced State API.

This library depends on functionstream-api (low-level). It provides
Codec, ValueState, ListState, MapState, PriorityQueueState,
AggregatingState, ReducingState, and all Keyed* state types.
"""

from .codec import (
    Codec,
    JsonCodec,
    PickleCodec,
    BytesCodec,
    StringCodec,
    BoolCodec,
    IntCodec,
    FloatCodec,
    default_codec_for,
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
    KeyedListStateFactory,
    KeyedValueStateFactory,
    KeyedMapStateFactory,
    KeyedPriorityQueueStateFactory,
    KeyedAggregatingStateFactory,
    KeyedReducingStateFactory,
    KeyedValueState,
    KeyedMapState,
    KeyedMapEntry,
    KeyedListState,
    KeyedPriorityQueueState,
    KeyedAggregatingState,
    KeyedReducingState,
)

__all__ = [
    "Codec",
    "JsonCodec",
    "PickleCodec",
    "BytesCodec",
    "StringCodec",
    "BoolCodec",
    "IntCodec",
    "FloatCodec",
    "default_codec_for",
    "ValueState",
    "ListState",
    "MapEntry",
    "MapState",
    "infer_ordered_key_codec",
    "create_map_state_auto_key_codec",
    "PriorityQueueState",
    "AggregateFunc",
    "AggregatingState",
    "ReduceFunc",
    "ReducingState",
    "KeyedListStateFactory",
    "KeyedValueStateFactory",
    "KeyedMapStateFactory",
    "KeyedPriorityQueueStateFactory",
    "KeyedAggregatingStateFactory",
    "KeyedReducingStateFactory",
    "KeyedValueState",
    "KeyedMapState",
    "KeyedMapEntry",
    "KeyedListState",
    "KeyedPriorityQueueState",
    "KeyedAggregatingState",
    "KeyedReducingState",
]
