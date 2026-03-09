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


class Float32Codec(Codec[float]):
    def encode(self, value: float) -> bytes:
        return struct.pack(">f", value)

    def decode(self, data: bytes) -> float:
        if len(data) != 4:
            raise ValueError(f"invalid float32 payload length: {len(data)}")
        return struct.unpack(">f", data)[0]
