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

from typing import Generic, TypeVar

T = TypeVar("T")


class Codec(Generic[T]):
    supports_ordered_keys: bool = False

    def encode(self, value: T) -> bytes:
        raise NotImplementedError

    def decode(self, data: bytes) -> T:
        raise NotImplementedError

    def encoded_size(self) -> int:
        """Return fixed encoded byte length; >0 means fixed-size, 0 or negative means variable length."""
        return 0
