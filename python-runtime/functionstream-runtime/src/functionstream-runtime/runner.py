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

import cloudpickle
from typing import Optional, List, Tuple, Dict

from functionstream_api.driver import FSProcessorDriver

from .store.fs_context import WitContext, convert_config_to_dict


_DRIVER: Optional[FSProcessorDriver] = None
_CONTEXT: Optional[WitContext] = None


class WitWorld:
    
    def fs_init(self, config: List[Tuple[str, str]]) -> None:
        global _DRIVER, _CONTEXT
        
        config_dict = convert_config_to_dict(config)
        
        _CONTEXT = WitContext(config_dict)
        
        if _DRIVER:
            try:
                _DRIVER.init(_CONTEXT, _CONTEXT._CONFIG)
            except Exception:
                pass
    
    def fs_process(self, source_id: int, data: bytes) -> None:
        global _DRIVER, _CONTEXT
        if not _DRIVER or not _CONTEXT:
            return
        
        try:
            _DRIVER.process(_CONTEXT, source_id, data)
        except Exception:
            pass
    
    def fs_process_watermark(self, source_id: int, watermark: int) -> None:
        global _DRIVER, _CONTEXT
        if not _DRIVER or not _CONTEXT:
            return
        
        try:
            _DRIVER.process_watermark(_CONTEXT, source_id, watermark)
        except Exception:
            pass
    
    def fs_take_checkpoint(self, checkpoint_id: int) -> None:
        global _DRIVER, _CONTEXT
        if not _DRIVER or not _CONTEXT:
            return
        
        try:
            _DRIVER.take_checkpoint(_CONTEXT, checkpoint_id)
        except Exception:
            pass
    
    def fs_check_heartbeat(self) -> bool:
        global _DRIVER, _CONTEXT
        if not _DRIVER or not _CONTEXT:
            return False
        
        try:
            return _DRIVER.check_heartbeat(_CONTEXT)
        except Exception:
            return False
    
    def fs_close(self) -> None:
        global _DRIVER, _CONTEXT
        
        if _DRIVER and _CONTEXT:
            try:
                _DRIVER.close(_CONTEXT)
            except Exception:
                pass
        
        _DRIVER = None
        _CONTEXT = None
    
    def fs_exec(self, payload: bytes) -> None:
        global _DRIVER
        
        _DRIVER = cloudpickle.loads(payload)
        
        if not isinstance(_DRIVER, FSProcessorDriver):
            raise TypeError("Payload is not a FSProcessorDriver instance")
    
    def fs_custom(self) -> bytes:
        global _DRIVER, _CONTEXT
        
        if not _DRIVER or not _CONTEXT:
            raise RuntimeError("Driver or Context not initialized")
        
        return _DRIVER.custom()


__all__ = ['WitWorld']
