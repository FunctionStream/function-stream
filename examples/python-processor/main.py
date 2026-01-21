
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
import logging
import sys
from pathlib import Path

import cloudpickle

from fs_api import FSProcessorDriver, Context
from fs_client.client import FsClient
import processor_impl
from serializer import serialize_by_value

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


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

    processor = processor_impl.CounterProcessor()
    cloudpickle.register_pickle_by_value(sys.modules[processor_impl.__name__])
    processor_bytes = serialize_by_value(processor)
    config_bytes = _read_file_bytes(args.config)

    with FsClient(host=args.host, port=args.port) as client:
        client.create_function_from_bytes(processor_bytes, config_bytes)
        logger.info("Python processor registered successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
