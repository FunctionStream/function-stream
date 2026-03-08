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

import json
import struct
from typing import Generic, TypeVar

import cloudpickle

T = TypeVar("T")


class Codec(Generic[T]):
    supports_ordered_keys: bool = False

    def encode(self, value: T) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes) -> T:
        raise NotImplementedError


class JsonCodec(Codec[T]):
    def encode(self, value: T) -> bytes:
        return json.dumps(value).encode("utf-8")

    def decode(self, data: bytes) -> T:
        return json.loads(data.decode("utf-8"))


class PickleCodec(Codec[T]):
    def encode(self, value: T) -> bytes:
        return cloudpickle.dumps(value)

    def decode(self, data: bytes) -> T:
        return cloudpickle.loads(data)


class BytesCodec(Codec[bytes]):
    supports_ordered_keys = True

    def encode(self, value: bytes) -> bytes:
        return bytes(value)

    def decode(self, data: bytes) -> bytes:
        return bytes(data)


class StringCodec(Codec[str]):
    supports_ordered_keys = True

    def encode(self, value: str) -> bytes:
        return value.encode("utf-8")

    def decode(self, data: bytes) -> str:
        return data.decode("utf-8")


class BoolCodec(Codec[bool]):
    supports_ordered_keys = True

    def encode(self, value: bool) -> bytes:
        return b"\x01" if value else b"\x00"

    def decode(self, data: bytes) -> bool:
        if len(data) != 1:
            raise ValueError(f"invalid bool payload length: {len(data)}")
        if data == b"\x00":
            return False
        if data == b"\x01":
            return True
        raise ValueError(f"invalid bool payload byte: {data[0]}")


class Int64Codec(Codec[int]):
    def encode(self, value: int) -> bytes:
        return struct.pack(">q", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 8:
            raise ValueError(f"invalid int64 payload length: {len(data)}")
        return struct.unpack(">q", data)[0]


class Uint64Codec(Codec[int]):
    def encode(self, value: int) -> bytes:
        if value < 0:
            raise ValueError("uint64 value must be >= 0")
        return struct.pack(">Q", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 8:
            raise ValueError(f"invalid uint64 payload length: {len(data)}")
        return struct.unpack(">Q", data)[0]


class Int32Codec(Codec[int]):
    def encode(self, value: int) -> bytes:
        return struct.pack(">i", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 4:
            raise ValueError(f"invalid int32 payload length: {len(data)}")
        return struct.unpack(">i", data)[0]


class Uint32Codec(Codec[int]):
    def encode(self, value: int) -> bytes:
        if value < 0:
            raise ValueError("uint32 value must be >= 0")
        return struct.pack(">I", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 4:
            raise ValueError(f"invalid uint32 payload length: {len(data)}")
        return struct.unpack(">I", data)[0]


class Float64Codec(Codec[float]):
    def encode(self, value: float) -> bytes:
        return struct.pack(">d", value)

    def decode(self, data: bytes) -> float:
        if len(data) != 8:
            raise ValueError(f"invalid float64 payload length: {len(data)}")
        return struct.unpack(">d", data)[0]


class Float32Codec(Codec[float]):
    def encode(self, value: float) -> bytes:
        return struct.pack(">f", value)

    def decode(self, data: bytes) -> float:
        if len(data) != 4:
            raise ValueError(f"invalid float32 payload length: {len(data)}")
        return struct.unpack(">f", data)[0]


class OrderedInt64Codec(Codec[int]):
    supports_ordered_keys = True

    def encode(self, value: int) -> bytes:
        mapped = (value & 0xFFFFFFFFFFFFFFFF) ^ (1 << 63)
        return struct.pack(">Q", mapped)

    def decode(self, data: bytes) -> int:
        if len(data) != 8:
            raise ValueError(f"invalid ordered int64 payload length: {len(data)}")
        mapped = struct.unpack(">Q", data)[0]
        raw = mapped ^ (1 << 63)
        if raw >= (1 << 63):
            return raw - (1 << 64)
        return raw


class OrderedUint64Codec(Codec[int]):
    supports_ordered_keys = True

    def encode(self, value: int) -> bytes:
        if value < 0:
            raise ValueError("ordered uint64 value must be >= 0")
        return struct.pack(">Q", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 8:
            raise ValueError(f"invalid ordered uint64 payload length: {len(data)}")
        return struct.unpack(">Q", data)[0]


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


class OrderedUint32Codec(Codec[int]):
    supports_ordered_keys = True

    def encode(self, value: int) -> bytes:
        if value < 0:
            raise ValueError("ordered uint32 value must be >= 0")
        return struct.pack(">I", value)

    def decode(self, data: bytes) -> int:
        if len(data) != 4:
            raise ValueError(f"invalid ordered uint32 payload length: {len(data)}")
        return struct.unpack(">I", data)[0]


class OrderedFloat64Codec(Codec[float]):
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
            raise ValueError(f"invalid ordered float64 payload length: {len(data)}")
        mapped = struct.unpack(">Q", data)[0]
        if mapped & (1 << 63):
            bits = mapped ^ (1 << 63)
        else:
            bits = (~mapped) & 0xFFFFFFFFFFFFFFFF
        return struct.unpack(">d", struct.pack(">Q", bits))[0]


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


__all__ = [
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
]
