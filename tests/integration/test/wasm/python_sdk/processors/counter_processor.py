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
A stateful processor that uses KVStore to count occurrences of incoming strings.
Validates state persistence and checkpointing mechanisms.
"""

import json
from typing import Dict

from fs_api import FSProcessorDriver, Context


class CounterProcessor(FSProcessorDriver):
    """
    Stateful word-counter backed by KVStore.
    Each incoming UTF-8 string increments its counter in the store and emits
    a JSON payload ``{"word": ..., "count": ..., "total": ...}`` downstream.
    """

    def __init__(self) -> None:
        self._counter_map: Dict[str, int] = {}
        self._total_processed: int = 0
        self._store_name: str = "integration-counter-store"

    def init(self, ctx: Context, config: dict) -> None:
        self._counter_map = {}
        self._total_processed = 0

    def process(self, ctx: Context, source_id: int, data: bytes) -> None:
        input_str = data.decode("utf-8", errors="replace").strip()
        if not input_str:
            return

        self._total_processed += 1
        store = ctx.getOrCreateKVStore(self._store_name)

        store_key_bytes = input_str.encode("utf-8")
        stored_val = store.get_state(store_key_bytes)
        current_count = int(stored_val.decode("utf-8")) if stored_val else 0
        new_count = current_count + 1

        self._counter_map[input_str] = new_count
        store.put_state(store_key_bytes, str(new_count).encode("utf-8"))

        payload = {"word": input_str, "count": new_count, "total": self._total_processed}
        ctx.emit(json.dumps(payload).encode("utf-8"), 0)

    def process_watermark(self, ctx: Context, source_id: int, watermark: int) -> None:
        ctx.emit_watermark(watermark, 0)

    def take_checkpoint(self, ctx: Context, checkpoint_id: int) -> bytes:
        return json.dumps(self._counter_map).encode("utf-8")

    def check_heartbeat(self, ctx: Context) -> bool:
        return True

    def close(self, ctx: Context) -> None:
        self._counter_map.clear()
        self._total_processed = 0

    def custom(self, payload: bytes) -> bytes:
        return b'{"status": "ok"}'
