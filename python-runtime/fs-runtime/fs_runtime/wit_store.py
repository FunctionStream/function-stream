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

"""
fs_runtime.wit_store

WIT kv.Store 的 Python 包装实现
"""
from typing import Optional, List
from functionstream_api.store import KvStore, ComplexKey, KvIterator
from functionstream_api.store.error import KvError, KvNotFoundError, KvIOError, KvOtherError


class WitKvStore(KvStore):
    """WIT kv.Store 的 Python 包装"""
    
    def __init__(self, store_resource):
        """
        初始化 WIT Store 包装
        
        Args:
            store_resource: WIT kv.Store 资源实例
        """
        self._store = store_resource
    
    def put_state(self, key: bytes, value: bytes):
        result = self._store.put_state(key, value)
        if hasattr(result, 'is_err') and result.is_err():
            err = result.unwrap_err() if hasattr(result, 'unwrap_err') else None
            if err == "not-found":
                raise KvNotFoundError()
            elif isinstance(err, tuple) and len(err) > 0:
                if err[0] == "io-error":
                    raise KvIOError(err[1] if len(err) > 1 else "IO error")
                elif err[0] == "other":
                    raise KvOtherError(err[1] if len(err) > 1 else "Other error")
            raise KvOtherError("Unknown error")
    
    def get_state(self, key: bytes) -> Optional[bytes]:
        result = self._store.get_state(key)
        if hasattr(result, 'is_err') and result.is_err():
            err = result.unwrap_err() if hasattr(result, 'unwrap_err') else None
            if err == "not-found":
                return None
            elif isinstance(err, tuple) and len(err) > 0:
                if err[0] == "io-error":
                    raise KvIOError(err[1] if len(err) > 1 else "IO error")
                elif err[0] == "other":
                    raise KvOtherError(err[1] if len(err) > 1 else "Other error")
            raise KvOtherError("Unknown error")
        if hasattr(result, 'unwrap'):
            option = result.unwrap()
            return option if option is not None else None
        return result if result is not None else None
    
    def delete_state(self, key: bytes):
        result = self._store.delete_state(key)
        if hasattr(result, 'is_err') and result.is_err():
            err = result.unwrap_err() if hasattr(result, 'unwrap_err') else None
            if isinstance(err, tuple) and len(err) > 0:
                if err[0] == "io-error":
                    raise KvIOError(err[1] if len(err) > 1 else "IO error")
                elif err[0] == "other":
                    raise KvOtherError(err[1] if len(err) > 1 else "Other error")
            raise KvOtherError("Unknown error")
    
    def list_states(self, start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        result = self._store.list_states(start_inclusive, end_exclusive)
        if hasattr(result, 'is_err') and result.is_err():
            err = result.unwrap_err() if hasattr(result, 'unwrap_err') else None
            if isinstance(err, tuple) and len(err) > 0:
                if err[0] == "io-error":
                    raise KvIOError(err[1] if len(err) > 1 else "IO error")
                elif err[0] == "other":
                    raise KvOtherError(err[1] if len(err) > 1 else "Other error")
            raise KvOtherError("Unknown error")
        if hasattr(result, 'unwrap'):
            return result.unwrap()
        return result if result is not None else []
    
    def put(self, key: ComplexKey, value: bytes):
        result = self._store.put(key, value)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("Put failed")
    
    def get(self, key: ComplexKey) -> Optional[bytes]:
        result = self._store.get(key)
        if hasattr(result, 'is_err') and result.is_err():
            return None
        if hasattr(result, 'unwrap'):
            return result.unwrap()
        return result if result is not None else None
    
    def delete(self, key: ComplexKey):
        result = self._store.delete(key)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("Delete failed")
    
    def merge(self, key: ComplexKey, value: bytes):
        result = self._store.merge(key, value)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("Merge failed")
    
    def delete_prefix(self, key: ComplexKey):
        result = self._store.delete_prefix(key)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("Delete prefix failed")
    
    def list_complex(self, key_group: bytes, key: bytes, namespace: bytes, 
                     start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        result = self._store.list_complex(key_group, key, namespace, start_inclusive, end_exclusive)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("List complex failed")
        if hasattr(result, 'unwrap'):
            return result.unwrap()
        return result if result is not None else []
    
    def scan_complex(self, key_group: bytes, key: bytes, namespace: bytes) -> KvIterator:
        result = self._store.scan_complex(key_group, key, namespace)
        if hasattr(result, 'is_err') and result.is_err():
            raise KvOtherError("Scan complex failed")
        if hasattr(result, 'unwrap'):
            return result.unwrap()
        raise KvOtherError("Scan complex returned invalid result")


__all__ = ['WitKvStore']

