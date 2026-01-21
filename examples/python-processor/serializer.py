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

import sys
from typing import Any

import cloudpickle


def serialize_by_value(obj: Any) -> bytes:
    """
    Serialize object by value (module code embedded) to avoid filesystem imports.
    """
    module_name = None
    if hasattr(obj, "__module__"):
        module_name = obj.__module__
    elif hasattr(obj, "__class__") and hasattr(obj.__class__, "__module__"):
        module_name = obj.__class__.__module__

    if not module_name or module_name == "__main__":
        return cloudpickle.dumps(obj)

    target_module = sys.modules.get(module_name)
    if target_module is None:
        raise ValueError(f"Module {module_name} not found in sys.modules")

    cloudpickle.register_pickle_by_value(target_module)

    base_package = module_name.split(".")[0]
    for name, mod in list(sys.modules.items()):
        if mod is not None and name.startswith(base_package):
            cloudpickle.register_pickle_by_value(mod)

    return cloudpickle.dumps(obj)

