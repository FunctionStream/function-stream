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
fs_api.store.store

WIT: resource kv.store
"""

import abc
from typing import Optional, List
from .complexkey import ComplexKey
from .iterator import KvIterator


class KvStore(abc.ABC):

    @abc.abstractmethod
    def put_state(self, key: bytes, value: bytes):
        pass

    @abc.abstractmethod
    def get_state(self, key: bytes) -> Optional[bytes]:
        pass

    @abc.abstractmethod
    def delete_state(self, key: bytes):
        pass

    @abc.abstractmethod
    def list_states(self, start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        pass

    @abc.abstractmethod
    def put(self, key: ComplexKey, value: bytes):
        pass

    @abc.abstractmethod
    def get(self, key: ComplexKey) -> Optional[bytes]:
        pass

    @abc.abstractmethod
    def delete(self, key: ComplexKey):
        pass

    @abc.abstractmethod
    def merge(self, key: ComplexKey, value: bytes):
        pass

    @abc.abstractmethod
    def delete_prefix(self, key: ComplexKey):
        pass

    @abc.abstractmethod
    def list_complex(self, key_group: bytes, key: bytes, namespace: bytes,
                     start_inclusive: bytes, end_exclusive: bytes) -> List[bytes]:
        pass


    @abc.abstractmethod
    def scan_complex(self, key_group: bytes, key: bytes, namespace: bytes) -> KvIterator:
        pass

__all__ = ['KvStore']

