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

import abc
from .context import Context

class FSProcessorDriver(abc.ABC):

    @abc.abstractmethod
    def init(self, ctx: Context, config: dict):
        pass

    @abc.abstractmethod
    def process(self, ctx: Context, source_id: int, data: bytes):
        pass
    
    @abc.abstractmethod
    def process_watermark(self, ctx: Context, source_id: int, watermark: int):
        pass

    @abc.abstractmethod
    def take_checkpoint(self, ctx: Context, checkpoint_id: int):
        pass

    @abc.abstractmethod
    def check_heartbeat(self, ctx: Context) -> bool:
        pass
    
    @abc.abstractmethod
    def close(self, ctx: Context):
        pass

    @abc.abstractmethod
    def custom(self, payload: bytes) -> bytes:
        pass

__all__ = ['FSProcessorDriver']

