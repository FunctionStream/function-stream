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
import logging

from fs_client.client import FsClient
from fs_client.config import WasmTaskBuilder, KafkaInput, KafkaOutput
import processor_impl

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Register python Processor via Function Stream python Client."
    )
    parser.add_argument("--host", default="localhost", help="Function Stream host")
    parser.add_argument("--port", type=int, default=8080, help="Function Stream port")
    args = parser.parse_args()

    logger.info("Connecting to Function Stream at %s:%s", args.host, args.port)

    config = (
        WasmTaskBuilder()
        .set_name("python-processor-example")
        .add_init_config("emit_threshold", "6")
        .add_init_config("class_name", "CounterProcessor")
        .add_input_group(
            [
                KafkaInput(
                    bootstrap_servers="localhost:9092",
                    topic="input-topic",
                    group_id="python-processor-group",
                    partition=0,
                )
            ]
        )
        .add_output(KafkaOutput("localhost:9092", "output-topic", 0))
        .build()
    )

    with FsClient(host=args.host, port=args.port) as client:
        client.create_python_function_from_config(config, processor_impl.CounterProcessor)
        logger.info("python processor registered successfully.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
