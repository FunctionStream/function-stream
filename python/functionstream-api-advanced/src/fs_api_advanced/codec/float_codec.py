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

from .base import Codec


class FloatCodec(Codec[float]):
    """Ordered float codec for range scans (lexicographic byte order)."""
    supports_ordered_keys = True

    def encode(self, value: float) -> bytes:
        bits = struct.unpack(">Q", struct.pack(">d", value))[0]
        if bits & (1 << 63):
            mapped = (~bits) & 0xFFFFFFFFFFFFFFFF
        else:
            mapped = bits ^ (1 << 63)
        return struct.pack(">Q", mapped)

    def decode(self, data: bytes) -> float:
        if len(data) != 8:
            raise ValueError(f"invalid float payload length: {len(data)}")
        mapped = struct.unpack(">Q", data)[0]
        if mapped & (1 << 63):
            bits = mapped ^ (1 << 63)
        else:
            bits = (~mapped) & 0xFFFFFFFFFFFFFFFF
        return struct.unpack(">d", struct.pack(">Q", bits))[0]
