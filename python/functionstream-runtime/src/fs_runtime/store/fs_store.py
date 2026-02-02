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

from typing import Optional, List, TYPE_CHECKING

from fs_api.store import KvStore, ComplexKey, KvIterator

from .fs_error import wit_to_api_error
from .fs_complex_key import api_to_wit
from .fs_iterator import FSIterator

if TYPE_CHECKING:
    from wit_world.imports.kv import Store as WitStore
else:
    try:
        from wit_world.imports.kv import Store as WitStore
    except (ImportError, AttributeError):
        WitStore = None


class FSStore(KvStore):

    def __init__(self, name: str):
        if WitStore is None:
            raise RuntimeError("WIT Store binding is not available")

        self._store: WitStore = WitStore(name)
        self._name = name

    def put_state(self, key: bytes, value: bytes) -> None:
        try:
            self._store.put_state(key, value)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def get_state(self, key: bytes) -> Optional[bytes]:
        try:
            return self._store.get_state(key)
        except Exception as e:
            api_error = wit_to_api_error(e)
            from fs_api.store.error import KvNotFoundError
            if isinstance(api_error, KvNotFoundError):
                return None
            raise api_error

    def delete_state(self, key: bytes) -> None:
        try:
            self._store.delete_state(key)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def list_states(self, start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        try:
            return self._store.list_states(start_inclusive, end_exclusive)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def put(self, key: ComplexKey, value: bytes) -> None:
        try:
            wit_key = api_to_wit(key)
            self._store.put(wit_key, value)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def get(self, key: ComplexKey) -> Optional[bytes]:
        try:
            wit_key = api_to_wit(key)
            return self._store.get(wit_key)
        except Exception as e:
            api_error = wit_to_api_error(e)
            from fs_api.store.error import KvNotFoundError
            if isinstance(api_error, KvNotFoundError):
                return None
            raise api_error

    def delete(self, key: ComplexKey) -> None:
        try:
            wit_key = api_to_wit(key)
            self._store.delete(wit_key)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def merge(self, key: ComplexKey, value: bytes) -> None:
        try:
            wit_key = api_to_wit(key)
            self._store.merge(wit_key, value)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def delete_prefix(self, key: ComplexKey) -> None:
        try:
            wit_key = api_to_wit(key)
            self._store.delete_prefix(wit_key)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def list_complex(self, key_group: bytes, key: bytes, namespace: bytes,
                     start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        try:
            return self._store.list_complex(key_group, key, namespace, start_inclusive, end_exclusive)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error

    def scan_complex(self, key_group: bytes, key: bytes, namespace: bytes) -> KvIterator:
        try:
            wit_iterator = self._store.scan_complex(key_group, key, namespace)
            return FSIterator(wit_iterator)
        except Exception as e:
            api_error = wit_to_api_error(e)
            raise api_error


__all__ = ['FSStore']

