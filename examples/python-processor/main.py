
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

"""
Python WASM Processor Example

componentize-py 会自动生成 WIT 绑定，导入路径是 wit_world.imports
"""

import json
from typing import Dict, List, Tuple

# 正确的导入路径：wit_world.imports（由 componentize-py 生成）
from wit_world.imports import kv, collector

# Global state
counter_map: Dict[str, int] = {}
_store = None
_total_processed: int = 0  # 总处理条数
_emit_threshold: int = 6   # 每处理多少条数据 emit 一次


class WitWorld:
    """实现 WIT processor world 的导出函数"""
    
    def fs_init(self, config: List[Tuple[str, str]]) -> None:
        """
        WIT: export fs-init: func(config: list<tuple<string, string>>);
        """
        global _store, counter_map, _total_processed, _emit_threshold
        
        # Initialize state
        counter_map = {}
        _total_processed = 0
        
        # Open state store using correct API: kv.Store(name)
        try:
            _store = kv.Store("counter-store")
        except Exception as e:
            _store = None

    def fs_process(self, source_id: int, data: bytes) -> None:
        """
        WIT: export fs-process: func(source-id: u32, data: list<u8>);
        注意：data 是 bytes 类型
        """
        global counter_map, _store, _total_processed, _emit_threshold
        
        try:
            # data 已经是 bytes 类型
            input_str = data.decode('utf-8')
            
            # 增加总处理条数
            _total_processed += 1
            
            # Get current count from store for this key
            count = 0
            if _store:
                try:
                    key_bytes = input_str.encode('utf-8')
                    result = _store.get_state(key_bytes)
                    if result is not None:
                        count = int(result.decode('utf-8'))
                except Exception:
                    pass
            
            # Increment count for this key
            count += 1
            counter_map[input_str] = count
            
            # Store updated count
            if _store:
                try:
                    key_bytes = input_str.encode('utf-8')
                    value_bytes = str(count).encode('utf-8')
                    _store.put_state(key_bytes, value_bytes)
                except Exception:
                    pass
            
            # 每处理 _emit_threshold 条数据就 emit 一次
            if _total_processed % _emit_threshold == 0:
                result_json = json.dumps({
                    "total_processed": _total_processed,
                    "counter_map": counter_map,
                })
                try:
                    collector.emit(0, result_json.encode('utf-8'))
                except Exception:
                    pass
                
        except Exception:
            pass

    def fs_process_watermark(self, source_id: int, watermark: int) -> None:
        """
        WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
        """
        pass

    def fs_take_checkpoint(self, checkpoint_id: int) -> bytes:
        """
        WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
        返回 bytes 类型
        """
        global counter_map
        try:
            return json.dumps(counter_map).encode('utf-8')
        except Exception:
            return b"{}"

    def fs_check_heartbeat(self) -> bool:
        """
        WIT: export fs-check-heartbeat: func() -> bool;
        """
        return True

    def fs_close(self) -> None:
        """
        WIT: export fs-close: func();
        """
        global _store, counter_map, _total_processed
        counter_map = {}
        _store = None
        _total_processed = 0

    def fs_exec_custom(self, payload: bytes) -> bytes:
        """
        WIT: export fs-exec-custom: func(payload: list<u8>) -> list<u8>;
        payload 和返回值都是 bytes 类型
        """
        global counter_map, _total_processed
        try:
            command = payload.decode('utf-8')
            if command == "get_stats":
                stats = {
                    "counter_map": counter_map,
                    "total_keys": len(counter_map),
                    "total_processed": _total_processed,
                }
                return json.dumps(stats).encode('utf-8')
            else:
                return b'{"error": "Unknown command"}'
        except Exception:
            return b'{"error": "Execution failed"}'
