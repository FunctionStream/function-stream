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

import argparse
import inspect
import logging
import pickletools
import sys
from pathlib import Path

import cloudpickle

from fs_api import FSProcessorDriver, Context
from fs_client.client import FsClient
import processor_impl
import serializer

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
        description="Register python Processor via Function Stream python Client."
    )
    parser.add_argument("--host", default="localhost", help="Function Stream host")
    parser.add_argument("--port", type=int, default=8080, help="Function Stream port")
    parser.add_argument("--config", type=Path, default=default_config, help="Config YAML path")
    args = parser.parse_args()

    logger.info("Connecting to Function Stream at %s:%s", args.host, args.port)
    logger.info("Config: %s", args.config)

    processor_impl_file = Path(inspect.getfile(processor_impl))
    logger.info("Reading processor implementation from: %s", processor_impl_file)

    # Read Python source code as UTF-8 bytes
    processor_source_bytes = _read_file_bytes(processor_impl_file)

    class_name = None
    for name, obj in inspect.getmembers(processor_impl):
        if (inspect.isclass(obj) and 
            issubclass(obj, FSProcessorDriver) and 
            obj is not FSProcessorDriver):
            class_name = name
            break
    
    if not class_name:
        raise RuntimeError("No FSProcessorDriver subclass found in processor_impl module")
    
    logger.info("Found processor class: %s", class_name)
    
    # Read config file and convert to string
    config_bytes = _read_file_bytes(args.config)
    config_content = config_bytes.decode("utf-8")
    
    # Build modules list: (module_name, module_bytes)
    # Use the module name "processor_impl" (without .py extension)
    module_name = processor_impl_file.stem  # "processor_impl"

    modules = [(module_name,processor_source_bytes)]
    
    logger.info("Prepared modules: %s (size: %d bytes)", module_name, len(processor_source_bytes))

    with FsClient(host=args.host, port=args.port) as client:
        client.create_python_function(class_name, modules, config_content)
        logger.info("python processor registered successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
