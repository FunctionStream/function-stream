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


class IntCodec(Codec[int]):
    supports_ordered_keys = True

    _PACKER = struct.Struct(">Q")
    _SIZE = 8

    def encoded_size(self) -> int:
        return self._SIZE
    _MASK_64 = 0xFFFFFFFFFFFFFFFF
    _SIGN_BIT = 1 << 63
    _TWO_TO_64 = 1 << 64

    def encode(self, value: int) -> bytes:
        if not (-self._SIGN_BIT <= value < self._SIGN_BIT):
            raise ValueError(f"Value out of 64-bit signed integer range: {value}")
        mapped = (value & self._MASK_64) ^ self._SIGN_BIT
        return self._PACKER.pack(mapped)

    def decode(self, data: bytes) -> int:
        try:
            mapped = self._PACKER.unpack(data)[0]
        except struct.error:
            raise ValueError(
                f"Invalid int payload length or format, expected 8 bytes, got {len(data)}"
            )
        raw = mapped ^ self._SIGN_BIT
        if raw >= self._SIGN_BIT:
            return raw - self._TWO_TO_64
        return raw
