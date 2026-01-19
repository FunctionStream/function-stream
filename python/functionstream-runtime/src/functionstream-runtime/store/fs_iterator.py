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

from typing import Optional, Tuple, TYPE_CHECKING

from functionstream_api.store.iterator import KvIterator
from functionstream_api.store.error import KvError

from .fs_error import wit_to_api_error

if TYPE_CHECKING:
    from wit_world.imports.kv import Iterator as WitIterator
else:
    try:
        from wit_world.imports.kv import Iterator as WitIterator
    except (ImportError, AttributeError):
        WitIterator = None


class FSIterator(KvIterator):
    
    def __init__(self, wit_iterator: 'WitIterator'):
        if WitIterator is None:
            raise RuntimeError("WIT Iterator binding is not available")
        if not isinstance(wit_iterator, WitIterator):
            raise TypeError(f"Expected WitIterator, got {type(wit_iterator)}")
        
        self._iterator: WitIterator = wit_iterator
    
    def has_next(self) -> bool:
        try:
            return self._iterator.has_next()
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error
    
    def next(self) -> Optional[Tuple[bytes, bytes]]:
        try:
            return self._iterator.next()
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error


__all__ = ['FSIterator']

