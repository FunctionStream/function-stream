
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
Python Processor Example using Function Stream Python Client.

Defines a FSProcessorDriver implementation, serializes it with cloudpickle,
and registers it with the server using create_function_from_bytes.
"""

import argparse
import json
import logging
from pathlib import Path
from typing import Dict

import cloudpickle
from fs_api import FSProcessorDriver, Context
from fs_client.client import FsClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


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
        try:
            command = payload.decode("utf-8") if payload else ""
        except UnicodeDecodeError:
            command = ""
        if command == "get_stats":
            return json.dumps(
                {
                    "total_processed": self._total_processed,
                    "counter_map": self._counter_map,
                }
            ).encode("utf-8")
        return b'{"error": "Unknown command"}'


def _read_file_bytes(path: Path) -> bytes:
    try:
        return path.read_bytes()
    except OSError as exc:
        raise RuntimeError(f"Failed to read file: {path}") from exc


def main() -> int:
    script_dir = Path(__file__).resolve().parent
    default_config = script_dir / "config.yaml"

    parser = argparse.ArgumentParser(
        description="Register Python Processor via Function Stream Python Client."
    )
    parser.add_argument("--host", default="localhost", help="Function Stream host")
    parser.add_argument("--port", type=int, default=8080, help="Function Stream port")
    parser.add_argument("--config", type=Path, default=default_config, help="Config YAML path")
    args = parser.parse_args()

    logger.info("Connecting to Function Stream at %s:%s", args.host, args.port)
    logger.info("Config: %s", args.config)

    processor = CounterProcessor()
    processor_bytes = cloudpickle.dumps(processor)
    config_bytes = _read_file_bytes(args.config)

    with FsClient(host=args.host, port=args.port) as client:
        client.create_function_from_bytes(processor_bytes, config_bytes)
        logger.info("Python processor registered successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
