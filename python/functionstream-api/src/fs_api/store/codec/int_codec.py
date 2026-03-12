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


# First byte: 0x00 = negative, 0x80 = non-negative (lexicographic order = numeric order).
_SIGN_NEG = 0x00
_SIGN_NONNEG = 0x80
# Legacy fixed 8-byte format length (for backward-compatible decode).
_LEGACY_LEN = 8
class IntCodec(Codec[int]):
    """Ordered int codec for range scans (lexicographic byte order).
    Supports arbitrary-precision Python int (32-bit, 64-bit, and larger).
    Encoding is variable-length; decode accepts both legacy 8-byte and new variable-length format.
    Exactly 8-byte payloads are always decoded as legacy; variable encoding never produces 8 bytes.
    """

    supports_ordered_keys = True

    def encode(self, value: int) -> bytes:
        if not isinstance(value, int):
            raise TypeError(f"expected int, got {type(value).__name__}")
        if value < 0:
            sign_byte = _SIGN_NEG
            mag = -(value + 1)  # -1 -> 0, -2 -> 1, ...
            n_bits = mag.bit_length() if mag else 0
            n_bytes = max(1, (n_bits + 7) // 8)
            if n_bytes > 255:
                raise OverflowError(
                    f"int magnitude requires {n_bytes} bytes; at most 255 bytes supported"
                )
            max_mag = (1 << (8 * n_bytes)) - 1
            mag_stored = max_mag - mag
            mag_bytes = mag_stored.to_bytes(n_bytes, "big")
        else:
            sign_byte = _SIGN_NONNEG
            mag = value
            n_bits = mag.bit_length() if mag else 0
            n_bytes = max(1, (n_bits + 7) // 8)
            if n_bytes > 255:
                raise OverflowError(
                    f"int magnitude requires {n_bytes} bytes; at most 255 bytes supported"
                )
            mag_bytes = mag.to_bytes(n_bytes, "big")
        # Avoid exactly 8 bytes so decode can treat all 8-byte payloads as legacy.
        if 2 + n_bytes == _LEGACY_LEN and n_bytes < 255:
            n_bytes += 1
            if sign_byte == _SIGN_NEG:
                max_mag = (1 << (8 * n_bytes)) - 1
                mag_stored = max_mag - mag
                mag_bytes = mag_stored.to_bytes(n_bytes, "big")
            else:
                mag_bytes = (0).to_bytes(1, "big") + mag_bytes
        return bytes([sign_byte, n_bytes]) + mag_bytes

    def decode(self, data: bytes) -> int:
        if len(data) == _LEGACY_LEN:
            return self._decode_legacy(data)
        return self._decode_variable(data)

    def _decode_legacy(self, data: bytes) -> int:
        """Decode legacy fixed 8-byte format for backward compatibility."""
        mapped = struct.unpack(">Q", data)[0]
        raw = mapped ^ (1 << 63)
        if raw >= (1 << 63):
            return raw - (1 << 64)
        return raw

    def _decode_variable(self, data: bytes) -> int:
        if len(data) < 2:
            raise ValueError("invalid int payload: too short (need at least 2 bytes)")
        sign_byte, n_bytes = data[0], data[1]
        if len(data) != 2 + n_bytes:
            raise ValueError(
                f"invalid int payload length: expected 2 + {n_bytes} = {2 + n_bytes}, got {len(data)}"
            )
        mag = int.from_bytes(data[2 : 2 + n_bytes], "big")
        if sign_byte == _SIGN_NEG:
            max_mag = (1 << (8 * n_bytes)) - 1
            mag = max_mag - mag
            return -(mag + 1)
        if sign_byte == _SIGN_NONNEG:
            return mag
        raise ValueError(f"invalid int payload: unknown sign byte 0x{sign_byte:02x}")
