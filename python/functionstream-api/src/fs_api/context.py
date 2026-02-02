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
fs_api.context

Context: Context object
"""
import abc
from typing import Dict
from .store import KvStore


class Context(abc.ABC):
    """Context object"""

    @abc.abstractmethod
    def emit(self, data: bytes, channel: int = 0):
        pass

    @abc.abstractmethod
    def emit_watermark(self, watermark: int, channel: int = 0):
        pass

    @abc.abstractmethod
    def getOrCreateKVStore(self, name: str) -> KvStore:
        pass

    @abc.abstractmethod
    def getConfig(self) -> Dict[str, str]:
        """
        Get global configuration Map

        Returns:
            Dict[str, str]: Configuration dictionary
        """
        pass

__all__ = ['Context']
