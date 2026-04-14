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
Unit tests for the Python SDK configuration layer:
WasmTaskBuilder, WasmTaskConfig, KafkaInput, KafkaOutput.

These tests do NOT require a running FunctionStream server.
"""

import pytest
import yaml

from fs_client.config import KafkaInput, KafkaOutput, WasmTaskBuilder, WasmTaskConfig


# ======================================================================
# KafkaInput
# ======================================================================


class TestKafkaInput:

    def test_basic_fields(self):
        ki = KafkaInput("broker:9092", "topic-a", "group-1")
        assert ki.data["input-type"] == "kafka"
        assert ki.data["bootstrap_servers"] == "broker:9092"
        assert ki.data["topic"] == "topic-a"
        assert ki.data["group_id"] == "group-1"
        assert "partition" not in ki.data

    def test_with_partition(self):
        ki = KafkaInput("broker:9092", "topic-a", "group-1", partition=3)
        assert ki.data["partition"] == 3

    def test_partition_zero_is_present(self):
        ki = KafkaInput("broker:9092", "t", "g", partition=0)
        assert ki.data["partition"] == 0

    def test_partition_none_is_absent(self):
        ki = KafkaInput("broker:9092", "t", "g", partition=None)
        assert "partition" not in ki.data


# ======================================================================
# KafkaOutput
# ======================================================================


class TestKafkaOutput:

    def test_basic_fields(self):
        ko = KafkaOutput("broker:9092", "out-topic", 1)
        assert ko.data["output-type"] == "kafka"
        assert ko.data["bootstrap_servers"] == "broker:9092"
        assert ko.data["topic"] == "out-topic"
        assert ko.data["partition"] == 1

    def test_partition_zero(self):
        ko = KafkaOutput("broker:9092", "t", 0)
        assert ko.data["partition"] == 0


# ======================================================================
# WasmTaskBuilder
# ======================================================================


class TestWasmTaskBuilder:

    def test_default_values(self):
        config = WasmTaskBuilder().build()
        assert config.task_name == "default-processor"
        assert config.task_type == "python"
        assert config.use_builtin_event_serialization is False
        assert config.enable_checkpoint is False
        assert config.checkpoint_interval_seconds == 1
        assert config.init_config == {}
        assert config.input_groups == []
        assert config.outputs == []

    def test_set_name(self):
        config = WasmTaskBuilder().set_name("my-fn").build()
        assert config.task_name == "my-fn"

    def test_empty_name_defaults(self):
        config = WasmTaskBuilder().set_name("").build()
        assert config.task_name == "default-processor"

    def test_blank_name_defaults(self):
        config = WasmTaskBuilder().set_name("   ").build()
        assert config.task_name == "default-processor"

    def test_set_type(self):
        config = WasmTaskBuilder().set_type("processor").build()
        assert config.task_type == "processor"

    def test_builtin_serialization(self):
        config = WasmTaskBuilder().set_builtin_serialization(True).build()
        assert config.use_builtin_event_serialization is True

    def test_configure_checkpoint_enabled(self):
        config = (
            WasmTaskBuilder()
            .configure_checkpoint(enabled=True, interval=5)
            .build()
        )
        assert config.enable_checkpoint is True
        assert config.checkpoint_interval_seconds == 5

    def test_checkpoint_interval_clamped_to_one(self):
        config = (
            WasmTaskBuilder()
            .configure_checkpoint(enabled=True, interval=0)
            .build()
        )
        assert config.checkpoint_interval_seconds == 1

    def test_checkpoint_interval_negative_clamped(self):
        config = (
            WasmTaskBuilder()
            .configure_checkpoint(enabled=True, interval=-10)
            .build()
        )
        assert config.checkpoint_interval_seconds == 1

    def test_add_init_config(self):
        config = (
            WasmTaskBuilder()
            .add_init_config("key1", "val1")
            .add_init_config("key2", "val2")
            .build()
        )
        assert config.init_config == {"key1": "val1", "key2": "val2"}

    def test_add_input_group(self):
        ki = KafkaInput("broker:9092", "in-topic", "grp")
        config = WasmTaskBuilder().add_input_group([ki]).build()
        assert len(config.input_groups) == 1
        assert len(config.input_groups[0]["inputs"]) == 1
        assert config.input_groups[0]["inputs"][0]["topic"] == "in-topic"

    def test_add_multiple_input_groups(self):
        ki1 = KafkaInput("b:9092", "t1", "g1")
        ki2 = KafkaInput("b:9092", "t2", "g2")
        config = (
            WasmTaskBuilder()
            .add_input_group([ki1])
            .add_input_group([ki2])
            .build()
        )
        assert len(config.input_groups) == 2

    def test_add_output(self):
        ko = KafkaOutput("broker:9092", "out", 0)
        config = WasmTaskBuilder().add_output(ko).build()
        assert len(config.outputs) == 1
        assert config.outputs[0]["topic"] == "out"

    def test_add_multiple_outputs(self):
        config = (
            WasmTaskBuilder()
            .add_output(KafkaOutput("b:9092", "o1", 0))
            .add_output(KafkaOutput("b:9092", "o2", 1))
            .build()
        )
        assert len(config.outputs) == 2

    def test_fluent_chaining(self):
        config = (
            WasmTaskBuilder()
            .set_name("chained")
            .set_type("python")
            .set_builtin_serialization(False)
            .configure_checkpoint(True, 10)
            .add_init_config("k", "v")
            .add_input_group([KafkaInput("b:9092", "t", "g")])
            .add_output(KafkaOutput("b:9092", "o", 0))
            .build()
        )
        assert config.task_name == "chained"
        assert config.enable_checkpoint is True
        assert config.checkpoint_interval_seconds == 10
        assert len(config.input_groups) == 1
        assert len(config.outputs) == 1

    def test_builder_produces_independent_configs(self):
        builder = WasmTaskBuilder().set_name("first")
        c1 = builder.build()
        builder.set_name("second")
        c2 = builder.build()
        assert c1.task_name == "first"
        assert c2.task_name == "second"


# ======================================================================
# WasmTaskConfig
# ======================================================================


class TestWasmTaskConfig:

    @pytest.fixture()
    def sample_config(self):
        return (
            WasmTaskBuilder()
            .set_name("test-processor")
            .set_type("python")
            .configure_checkpoint(True, 5)
            .add_init_config("key", "value")
            .add_input_group(
                [KafkaInput("localhost:9092", "in-topic", "test-group", partition=0)]
            )
            .add_output(KafkaOutput("localhost:9092", "out-topic", 0))
            .build()
        )

    def test_to_dict_keys(self, sample_config):
        d = sample_config.to_dict()
        assert "name" in d
        assert "type" in d
        assert "use_builtin_event_serialization" in d
        assert "enable_checkpoint" in d
        assert "checkpoint_interval_seconds" in d
        assert "init_config" in d
        assert "input-groups" in d
        assert "outputs" in d

    def test_to_dict_values(self, sample_config):
        d = sample_config.to_dict()
        assert d["name"] == "test-processor"
        assert d["type"] == "python"
        assert d["enable_checkpoint"] is True
        assert d["checkpoint_interval_seconds"] == 5

    def test_to_yaml_produces_valid_yaml(self, sample_config):
        yaml_str = sample_config.to_yaml()
        parsed = yaml.safe_load(yaml_str)
        assert isinstance(parsed, dict)
        assert parsed["name"] == "test-processor"

    def test_from_yaml_roundtrip(self, sample_config):
        yaml_str = sample_config.to_yaml()
        restored = WasmTaskConfig.from_yaml(yaml_str)
        assert restored.task_name == sample_config.task_name
        assert restored.task_type == sample_config.task_type
        assert restored.enable_checkpoint == sample_config.enable_checkpoint
        assert restored.checkpoint_interval_seconds == sample_config.checkpoint_interval_seconds

    def test_from_yaml_minimal(self):
        cfg = WasmTaskConfig.from_yaml("name: mini\n")
        assert cfg.task_name == "mini"
        assert cfg.task_type == "python"
        assert cfg.enable_checkpoint is False

    def test_from_yaml_missing_name_gets_default(self):
        cfg = WasmTaskConfig.from_yaml("type: python\n")
        assert cfg.task_name == "default-processor"

    def test_from_yaml_empty_name_gets_default(self):
        cfg = WasmTaskConfig.from_yaml("name: ''\n")
        assert cfg.task_name == "default-processor"

    def test_from_yaml_invalid_format_raises(self):
        with pytest.raises(ValueError, match="mapping"):
            WasmTaskConfig.from_yaml("- just\n- a\n- list\n")

    def test_from_yaml_checkpoint_interval_clamped(self):
        cfg = WasmTaskConfig.from_yaml(
            "name: x\ncheckpoint_interval_seconds: 0\n"
        )
        assert cfg.checkpoint_interval_seconds >= 1

    def test_from_yaml_with_init_config(self):
        cfg = WasmTaskConfig.from_yaml(
            "name: x\ninit_config:\n  a: '1'\n  b: '2'\n"
        )
        assert cfg.init_config == {"a": "1", "b": "2"}

    def test_to_yaml_sort_keys_false(self, sample_config):
        yaml_str = sample_config.to_yaml()
        lines = yaml_str.strip().split("\n")
        first_key = lines[0].split(":")[0]
        assert first_key == "name"
