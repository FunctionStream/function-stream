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

from typing import Any

from ..error import KvError

KEYED_VALUE_GROUP = b"__fssdk__/keyed/value"
KEYED_LIST_GROUP = b"__fssdk__/keyed/list"
KEYED_MAP_GROUP = b"__fssdk__/keyed/map"
KEYED_PQ_GROUP = b"__fssdk__/keyed/pq"
KEYED_AGGREGATING_GROUP = b"__fssdk__/keyed/aggregating"
KEYED_REDUCING_GROUP = b"__fssdk__/keyed/reducing"


def ensure_ordered_key_codec(codec: Any, label: str) -> None:
    if not getattr(codec, "supports_ordered_keys", False):
        raise KvError(f"{label} key codec must support ordered key encoding")


__all__ = [
    "ensure_ordered_key_codec",
    "KEYED_VALUE_GROUP",
    "KEYED_LIST_GROUP",
    "KEYED_MAP_GROUP",
    "KEYED_PQ_GROUP",
    "KEYED_AGGREGATING_GROUP",
    "KEYED_REDUCING_GROUP",
]
