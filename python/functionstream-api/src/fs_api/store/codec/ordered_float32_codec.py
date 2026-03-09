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


class OrderedFloat32Codec(Codec[float]):
    supports_ordered_keys = True

    def encode(self, value: float) -> bytes:
        bits = struct.unpack(">I", struct.pack(">f", value))[0]
        if bits & (1 << 31):
            mapped = (~bits) & 0xFFFFFFFF
        else:
            mapped = bits ^ (1 << 31)
        return struct.pack(">I", mapped)

    def decode(self, data: bytes) -> float:
        if len(data) != 4:
            raise ValueError(f"invalid ordered float32 payload length: {len(data)}")
        mapped = struct.unpack(">I", data)[0]
        if mapped & (1 << 31):
            bits = mapped ^ (1 << 31)
        else:
            bits = (~mapped) & 0xFFFFFFFF
        return struct.unpack(">f", struct.pack(">I", bits))[0]
