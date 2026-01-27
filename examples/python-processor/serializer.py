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
import pickletools
import sys
from typing import Any, Tuple

import cloudpickle

processor_code = '''
import json
from typing import Dict

from fs_api import FSProcessorDriver, Context

class CounterProcessor(FSProcessorDriver):
    """
    Real-time Counter Processor.
    Function: Updates state and emits a snapshot for EVERY single message processed.
    """
    def __init__(self) -> None:
        self._counter_map: Dict[str, int] = {}
        self._total_processed: int = 0
        self._store_name: str = "counter-store"
        self._key_prefix: str = ""

    def init(self, ctx: Context, config: dict) -> None:
        """Initialize the processor."""
        self._counter_map = {}
        self._total_processed = 0

        # Safe configuration parsing
        if isinstance(config, dict):
            # Parse Key prefix
            self._key_prefix = str(config.get("key_prefix", ""))

    def process(self, ctx: Context, source_id: int, data: bytes) -> None:
        """Core processing logic: One input -> One output."""
        try:
            # 1. Decode data
            input_str = data.decode("utf-8", errors="replace").strip()
            if not input_str:
                return

            self._total_processed += 1

            # 2. State Management (Load -> Modify -> Store)
            store = ctx.getOrCreateKVStore(self._store_name)

            full_key = f"{self._key_prefix}{input_str}"
            store_key_bytes = full_key.encode("utf-8")

            current_count = 0
            stored_val = store.get_state(store_key_bytes)

            if stored_val:
                try:
                    current_count = int(stored_val.decode("utf-8"))
                except ValueError:
                    current_count = 0

            new_count = current_count + 1

            # Update memory and persistence
            self._counter_map[input_str] = new_count
            store.put_state(store_key_bytes, str(new_count).encode("utf-8"))

            # 3. Trigger Output Emission
            # REQUIREMENT: Emit output for every single message processed
            self._emit_snapshot(ctx)

        except Exception as e:
            raise e

    def _emit_snapshot(self, ctx: Context):
        """Helper: Serialize and emit current state."""
        output_payload = {
            "total_processed": self._total_processed,
            "counter_map": self._counter_map
        }

        payload_bytes = json.dumps(output_payload).encode("utf-8")
        ctx.emit(payload_bytes, 0)

    def process_watermark(self, ctx: Context, source_id: int, watermark: int):
        ctx.emit_watermark(watermark, 0)

    def take_checkpoint(self, ctx: Context, checkpoint_id: int):
        return None

    def check_heartbeat(self, ctx: Context) -> bool:
        return True

    def close(self, ctx: Context):
        self._counter_map = {}
        self._total_processed = 0

    def custom(self, payload: bytes) -> bytes:
        return b'{"error": "Unknown command"}'
'''

def serialize_by_value(obj: Any) -> Tuple[str, bytes]:
    """
    Serialize object by value (module code embedded) to avoid filesystem imports.

    Uses cloudpickle.register_pickle_by_value(module) then cloudpickle.dumps(obj).
    Returns (module_name, serialized_bytes).
    """
    module_name = None
    if hasattr(obj, "__module__"):
        module_name = obj.__module__
    elif hasattr(obj, "__class__") and hasattr(obj.__class__, "__module__"):
        module_name = obj.__class__.__module__

    if not module_name or module_name == "__main__":
        return (module_name or "__main__", cloudpickle.dumps(obj))

    target_module = sys.modules.get(module_name)
    if target_module is None:
        raise ValueError(f"Module {module_name} not found in sys.modules")

    cloudpickle.register_pickle_by_value(target_module)
    base_package = module_name.split(".")[0]
    for name, mod in list(sys.modules.items()):
        if mod is not None and name.startswith(base_package):
            cloudpickle.register_pickle_by_value(mod)
    local_env = {}
    global_env = globals().copy()
    exec(processor_code,local_env)
    del local_env['__builtins__']
    payload = cloudpickle.dumps(local_env)
    pickletools.dis(payload)
    return (module_name, payload)

