# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from .fs_error import wit_to_api_error, api_to_wit_error
from .fs_complex_key import api_to_wit, wit_to_api
from .fs_iterator import FSIterator
from .fs_store import FSStore
from .fs_collector import emit, emit_watermark
from .fs_context import WitContext, convert_config_to_dict

try:
    from wit_world.imports import kv, collector
    _WIT_AVAILABLE = True
except ImportError:
    kv = None
    collector = None
    _WIT_AVAILABLE = False

__all__ = [
    'wit_to_api_error', 'api_to_wit_error',
    'api_to_wit', 'wit_to_api',
    'FSIterator', 'FSStore',
    'emit', 'emit_watermark',
    'WitContext', 'convert_config_to_dict',
    'kv', 'collector',
]

