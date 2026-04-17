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
import logging
import time
import uuid
from dataclasses import dataclass
from typing import Any, Dict, List

from confluent_kafka import Consumer, KafkaError, Producer
from fs_client.client import FsClient
from fs_client.config import KafkaInput, KafkaOutput, WasmTaskBuilder
from processors.counter_processor import CounterProcessor

logger = logging.getLogger(__name__)

CONSUME_TIMEOUT_S = 60.0
POLL_INTERVAL_S = 0.5
CONSUMER_WARMUP_S = 3.0


@dataclass(frozen=True)
class FlowContext:
    fn_name: str
    in_topic: str
    out_topic: str


def _unique_id(prefix: str) -> str:
    return f"{prefix}-{uuid.uuid4().hex[:8]}"


def _build_earliest_input(bootstrap: str, topic: str, group: str) -> KafkaInput:
    ki = KafkaInput(bootstrap, topic, group, 0)
    ki.data["auto.offset.reset"] = "earliest"
    return ki


def produce_messages(bootstrap: str, topic: str, messages: List[str], timeout: float = 10.0) -> None:
    producer = Producer({"bootstrap.servers": bootstrap})
    try:
        for msg in messages:
            producer.produce(topic, value=msg.encode("utf-8"))
    finally:
        remaining = producer.flush(timeout=timeout)
        if remaining > 0:
            raise RuntimeError(f"Producer failed to flush {remaining} messages within {timeout}s")


def consume_messages(
        bootstrap: str,
        topic: str,
        expected_count: int,
        timeout: float = CONSUME_TIMEOUT_S,
) -> List[Dict[str, Any]]:
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": _unique_id("test-consumer"),
        "auto.offset.reset": "earliest",
        "enable.auto.commit": "false",
    })
    consumer.subscribe([topic])
    collected: List[Dict[str, Any]] = []
    deadline = time.time() + timeout

    try:
        while len(collected) < expected_count and time.time() < deadline:
            msg = consumer.poll(timeout=POLL_INTERVAL_S)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logger.error("Kafka consumer error: %s", msg.error())
                continue

            payload = msg.value().decode("utf-8")
            collected.append(json.loads(payload))
    finally:
        consumer.close()

    if len(collected) < expected_count:
        raise TimeoutError(f"Expected {expected_count} messages, received {len(collected)}")

    return collected


def deploy_function(
        fs_client: FsClient,
        fn_name: str,
        bootstrap: str,
        in_topic: str,
        out_topic: str,
) -> None:
    config = (
        WasmTaskBuilder()
        .set_name(fn_name)
        .add_init_config("class_name", "CounterProcessor")
        .add_input_group([_build_earliest_input(bootstrap, in_topic, fn_name)])
        .add_output(KafkaOutput(bootstrap, out_topic, 0))
        .build()
    )

    success = fs_client.create_python_function_from_config(config, CounterProcessor)
    if not success:
        raise RuntimeError(f"Failed to deploy function: {fn_name}")

    time.sleep(CONSUMER_WARMUP_S)


class TestDataFlow:

    def _setup_flow(self, function_registry: List[str], kafka: Any, prefix: str) -> FlowContext:
        fn_name = _unique_id(prefix)
        ctx = FlowContext(
            fn_name=fn_name,
            in_topic=f"{fn_name}-in",
            out_topic=f"{fn_name}-out"
        )
        function_registry.append(ctx.fn_name)
        kafka.create_topics_if_not_exist([ctx.in_topic, ctx.out_topic])
        return ctx

    def test_single_word_counting(
            self,
            fs_client: FsClient,
            function_registry: List[str],
            kafka: Any,
            kafka_topics: str,
    ):
        ctx = self._setup_flow(function_registry, kafka, "flow-single")
        word = "hello"
        n = 10

        produce_messages(kafka_topics, ctx.in_topic, [word] * n)
        deploy_function(fs_client, ctx.fn_name, kafka_topics, ctx.in_topic, ctx.out_topic)
        records = consume_messages(kafka_topics, ctx.out_topic, n)

        for i, rec in enumerate(records, start=1):
            assert rec["word"] == word
            assert rec["count"] == i
            assert rec["total"] == i

    def test_multiple_distinct_words(
            self,
            fs_client: FsClient,
            function_registry: List[str],
            kafka: Any,
            kafka_topics: str,
    ):
        ctx = self._setup_flow(function_registry, kafka, "flow-multi")
        messages = ["apple", "banana", "apple", "cherry", "banana", "apple"]

        produce_messages(kafka_topics, ctx.in_topic, messages)
        deploy_function(fs_client, ctx.fn_name, kafka_topics, ctx.in_topic, ctx.out_topic)
        records = consume_messages(kafka_topics, ctx.out_topic, len(messages))

        per_word_counts: Dict[str, int] = {}
        for rec in records:
            word = rec["word"]
            per_word_counts[word] = per_word_counts.get(word, 0) + 1
            assert rec["count"] == per_word_counts[word]

        assert per_word_counts == {"apple": 3, "banana": 2, "cherry": 1}

    def test_large_batch_throughput(
            self,
            fs_client: FsClient,
            function_registry: List[str],
            kafka: Any,
            kafka_topics: str,
    ):
        ctx = self._setup_flow(function_registry, kafka, "flow-batch")
        batch_size = 500
        messages = [f"item-{i % 50}" for i in range(batch_size)]

        produce_messages(kafka_topics, ctx.in_topic, messages)
        deploy_function(fs_client, ctx.fn_name, kafka_topics, ctx.in_topic, ctx.out_topic)
        records = consume_messages(kafka_topics, ctx.out_topic, batch_size)

        assert len(records) == batch_size
        totals = [r["total"] for r in records]
        assert totals == list(range(1, batch_size + 1))

        per_word: Dict[str, int] = {}
        for rec in records:
            word = rec["word"]
            per_word[word] = per_word.get(word, 0) + 1
            assert rec["count"] == per_word[word]

    def test_empty_messages_are_skipped(
            self,
            fs_client: FsClient,
            function_registry: List[str],
            kafka: Any,
            kafka_topics: str,
    ):
        ctx = self._setup_flow(function_registry, kafka, "flow-empty")
        messages = ["foo", "", "bar", "  ", "foo", ""]

        produce_messages(kafka_topics, ctx.in_topic, messages)
        deploy_function(fs_client, ctx.fn_name, kafka_topics, ctx.in_topic, ctx.out_topic)

        expected_outputs = 3
        records = consume_messages(kafka_topics, ctx.out_topic, expected_outputs)

        words = [r["word"] for r in records]
        assert words == ["foo", "bar", "foo"]
        assert records[0]["count"] == 1
        assert records[1]["count"] == 1
        assert records[2]["count"] == 2

    def test_output_json_schema(
            self,
            fs_client: FsClient,
            function_registry: List[str],
            kafka: Any,
            kafka_topics: str,
    ):
        ctx = self._setup_flow(function_registry, kafka, "flow-schema")
        test_words = ["alpha", "beta", "gamma"]

        produce_messages(kafka_topics, ctx.in_topic, test_words)
        deploy_function(fs_client, ctx.fn_name, kafka_topics, ctx.in_topic, ctx.out_topic)
        records = consume_messages(kafka_topics, ctx.out_topic, len(test_words))

        for rec in records:
            assert set(rec.keys()) == {"word", "count", "total"}
            assert isinstance(rec["word"], str)
            assert isinstance(rec["count"], int)
            assert isinstance(rec["total"], int)
            assert rec["count"] >= 1
            assert rec["total"] >= 1