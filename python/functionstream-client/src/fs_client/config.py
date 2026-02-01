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

from typing import Dict, List, Tuple, Optional


class Config:

    def __init__(self, config_dict: Dict[str, str]):
        self._config: Dict[str, str] = dict(config_dict) if config_dict else {}

    def get(self, key: str, default: Optional[str] = None) -> Optional[str]:
        return self._config.get(key, default)

    def get_all(self) -> Dict[str, str]:
        return self._config.copy()

    def to_list(self) -> List[Tuple[str, str]]:
        return [(k, v) for k, v in self._config.items()]

    @classmethod
    def builder(cls) -> "ConfigBuilder":
        return ConfigBuilder()


class ConfigBuilder:

    def __init__(self):
        self._config: Dict[str, str] = {}

    def with_config(self, key: str, value: str) -> "ConfigBuilder":
        self._config[str(key)] = str(value)
        return self

    def with_configs(self, config_dict: Dict[str, str]) -> "ConfigBuilder":
        for k, v in config_dict.items():
            self._config[str(k)] = str(v)
        return self

    def with_config_list(self, items: List[Tuple[str, str]]) -> "ConfigBuilder":
        for item in items:
            if isinstance(item, (list, tuple)) and len(item) >= 2:
                self._config[str(item[0])] = str(item[1])
        return self

    def clear_config(self, key: Optional[str] = None) -> "ConfigBuilder":
        if key is None:
            self._config.clear()
        else:
            self._config.pop(str(key), None)
        return self

    def build(self) -> Config:
        return Config(self._config.copy())


__all__ = ["Config", "ConfigBuilder"]
