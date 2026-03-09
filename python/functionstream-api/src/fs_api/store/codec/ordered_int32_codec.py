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


class OrderedInt32Codec(Codec[int]):
    supports_ordered_keys = True

    def encode(self, value: int) -> bytes:
        mapped = (value & 0xFFFFFFFF) ^ (1 << 31)
        return struct.pack(">I", mapped)

    def decode(self, data: bytes) -> int:
        if len(data) != 4:
            raise ValueError(f"invalid ordered int32 payload length: {len(data)}")
        mapped = struct.unpack(">I", data)[0]
        raw = mapped ^ (1 << 31)
        if raw >= (1 << 31):
            return raw - (1 << 32)
        return raw
