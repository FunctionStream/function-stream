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

import json
from typing import Dict

from fs_api import FSProcessorDriver, Context


class CounterProcessor(FSProcessorDriver):
    def __init__(self) -> None:
        self._counter_map: Dict[str, int] = {}
        self._total_processed = 0
        self._emit_threshold = 6
        self._store_name = "counter-store"

    def init(self, ctx: Context, config: dict):
        self._counter_map = {}
        self._total_processed = 0
        threshold = config.get("emit_threshold") if isinstance(config, dict) else None
        if threshold is not None:
            try:
                self._emit_threshold = max(1, int(threshold))
            except (TypeError, ValueError):
                self._emit_threshold = 6

    def process(self, ctx: Context, source_id: int, data: bytes):
        input_str = data.decode("utf-8", errors="ignore")
        if not input_str:
            return

        self._total_processed += 1
        store = ctx.getOrCreateKVStore(self._store_name)

        count = 0
        stored = store.get_state(input_str.encode("utf-8"))
        if stored is not None:
            try:
                count = int(stored.decode("utf-8"))
            except (TypeError, ValueError):
                count = 0

        count += 1
        self._counter_map[input_str] = count
        store.put_state(input_str.encode("utf-8"), str(count).encode("utf-8"))

        if self._total_processed % self._emit_threshold == 0:
            payload = json.dumps(
                {
                    "total_processed": self._total_processed,
                    "counter_map": self._counter_map,
                }
            ).encode("utf-8")
            ctx.emit(payload, 0)

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


