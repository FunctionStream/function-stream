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
from typing import Tuple

from ..error import KvError

VALUE_PREFIX = b"__fssdk__/value/"
LIST_PREFIX = b"__fssdk__/list/"
PQ_PREFIX = b"__fssdk__/priority_queue/"
AGGREGATING_PREFIX = b"__fssdk__/aggregating/"
REDUCING_PREFIX = b"__fssdk__/reducing/"
MAP_GROUP = b"__fssdk__/map"
LIST_GROUP = b"__fssdk__/list"
PQ_GROUP = b"__fssdk__/priority_queue"


def validate_state_name(name: str) -> None:
    if not isinstance(name, str) or not name.strip():
        raise KvError("state name must be a non-empty string")


def encode_int64_lex(value: int) -> bytes:
    mapped = (value & 0xFFFFFFFFFFFFFFFF) ^ (1 << 63)
    return struct.pack(">Q", mapped)


def encode_priority_key(priority: int, seq: int) -> bytes:
    return encode_int64_lex(priority) + struct.pack(">Q", seq)


def decode_priority_key(data: bytes) -> Tuple[int, int]:
    if len(data) != 16:
        raise KvError("invalid priority queue key length")
    mapped_priority = struct.unpack(">Q", data[:8])[0]
    unsigned_priority = mapped_priority ^ (1 << 63)
    if unsigned_priority >= (1 << 63):
        priority = unsigned_priority - (1 << 64)
    else:
        priority = unsigned_priority
    seq = struct.unpack(">Q", data[8:])[0]
    return priority, seq


__all__ = [
    "VALUE_PREFIX",
    "LIST_PREFIX",
    "PQ_PREFIX",
    "AGGREGATING_PREFIX",
    "REDUCING_PREFIX",
    "MAP_GROUP",
    "LIST_GROUP",
    "PQ_GROUP",
    "validate_state_name",
    "encode_int64_lex",
    "encode_priority_key",
    "decode_priority_key",
]
