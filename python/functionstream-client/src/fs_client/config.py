# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# you may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from typing import Any, Dict, List, Optional

import yaml

# ==========================================
# 1. Models mapping to Rust Input/Output
# ==========================================

class KafkaInput:
    def __init__(self, bootstrap_servers: str, topic: str, group_id: str, partition: Optional[int] = None):
        self.data = {
            "input-type": "kafka",
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "group_id": group_id,
        }
        if partition is not None:
            self.data["partition"] = partition

class KafkaOutput:
    def __init__(self, bootstrap_servers: str, topic: str, partition: int):
        self.data = {
            "output-type": "kafka",
            "bootstrap_servers": bootstrap_servers,
            "topic": topic,
            "partition": partition,
        }

# ==========================================
# 2. WasmTaskConfig Object
# ==========================================

class WasmTaskConfig:
    def __init__(
        self,
        task_name: str,
        task_type: str,
        input_groups: List[Dict],
        use_builtin: bool,
        enable_checkpoint: bool,
        checkpoint_interval: int,
        init_config: Dict[str, str],
        outputs: List[Dict]
    ):
        self.task_name = task_name
        self.task_type = task_type
        self.input_groups = input_groups
        self.use_builtin_event_serialization = use_builtin
        self.enable_checkpoint = enable_checkpoint
        self.checkpoint_interval_seconds = checkpoint_interval
        self.init_config = init_config
        self.outputs = outputs

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.task_name,
            "type": self.task_type,
            "use_builtin_event_serialization": self.use_builtin_event_serialization,
            "enable_checkpoint": self.enable_checkpoint,
            "checkpoint_interval_seconds": self.checkpoint_interval_seconds,
            "init_config": self.init_config,
            "input-groups": self.input_groups,
            "outputs": self.outputs,
        }

    def to_yaml(self) -> str:
        return yaml.dump(self.to_dict(), sort_keys=False, allow_unicode=True, indent=2)

    @classmethod
    def from_yaml(cls, yaml_str: str) -> "WasmTaskConfig":
        data = yaml.safe_load(yaml_str)
        if not isinstance(data, dict):
            raise ValueError("Config YAML must be a mapping")
        name = data.get("name") or "default-processor"
        if isinstance(name, str) and not name.strip():
            name = "default-processor"
        return cls(
            task_name=name,
            task_type=data.get("type") or "python",
            input_groups=data.get("input-groups") or [],
            use_builtin=data.get("use_builtin_event_serialization", False),
            enable_checkpoint=data.get("enable_checkpoint", False),
            checkpoint_interval=max(1, data.get("checkpoint_interval_seconds", 1)),
            init_config=data.get("init_config") or {},
            outputs=data.get("outputs") or [],
        )

# ==========================================
# 3. Builder Implementation
# ==========================================

class WasmTaskBuilder:
    def __init__(self):
        self._name: Optional[str] = None
        self._type: str = "python"
        self._use_builtin: bool = False
        self._enable_checkpoint: bool = False
        self._checkpoint_interval: int = 1
        self._init_config: Dict[str, str] = {}
        self._input_groups_data: List[Dict] = []
        self._outputs_data: List[Dict] = []

    def set_name(self, name: str):
        self._name = name
        return self

    def set_type(self, task_type: str):
        self._type = task_type
        return self

    def set_builtin_serialization(self, enabled: bool):
        self._use_builtin = enabled
        return self

    def configure_checkpoint(self, enabled: bool, interval: int = 1):
        self._enable_checkpoint = enabled
        self._checkpoint_interval = max(1, interval)
        return self

    def add_init_config(self, key: str, value: str):
        self._init_config[key] = value
        return self

    def add_input_group(self, inputs: List[KafkaInput]):
        self._input_groups_data.append({
            "inputs": [item.data for item in inputs]
        })
        return self

    def add_output(self, output: KafkaOutput):
        self._outputs_data.append(output.data)
        return self

    def build(self) -> WasmTaskConfig:
        final_name = self._name if (self._name and self._name.strip()) else "default-processor"

        return WasmTaskConfig(
            task_name=final_name,
            task_type=self._type,
            input_groups=self._input_groups_data,
            use_builtin=self._use_builtin,
            enable_checkpoint=self._enable_checkpoint,
            checkpoint_interval=self._checkpoint_interval,
            init_config=self._init_config,
            outputs=self._outputs_data
        )
