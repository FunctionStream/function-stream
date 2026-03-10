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

from typing import Any, Type

from .base import Codec
from .bool_codec import BoolCodec
from .bytes_codec import BytesCodec
from .float_codec import FloatCodec
from .int_codec import IntCodec
from .json_codec import JsonCodec
from .pickle_codec import PickleCodec
from .string_codec import StringCodec


def default_codec_for(value_type: Type[Any]) -> Codec[Any]:
    """
    Return a default Codec for the given type, aligned with Go DefaultCodecFor.
    Built-in types use ordered codecs where applicable; list/dict use JsonCodec; else PickleCodec.
    """
    if value_type is bool:
        return BoolCodec()
    if value_type is int:
        return IntCodec()
    if value_type is float:
        return FloatCodec()
    if value_type is str:
        return StringCodec()
    if value_type is bytes:
        return BytesCodec()
    try:
        if issubclass(value_type, int) and value_type is not bool:
            return IntCodec()
    except TypeError:
        pass
    if value_type is list or value_type is dict:
        return JsonCodec()
    return PickleCodec()
