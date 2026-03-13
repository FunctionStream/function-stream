# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import importlib.util
import logging
import json
import sys
from typing import List, Optional, Tuple

from fs_api.driver import FSProcessorDriver

logger = logging.getLogger(__name__)

from .store.fs_context import WitContext, convert_config_to_dict
from fs_api_advanced import (
    Codec,
    ValueState,
    MapState,
    ListState,
    PriorityQueueState,
    AggregatingState,
    ReducingState,
    KeyedListStateFactory,
    KeyedValueStateFactory,
    KeyedMapStateFactory,
    KeyedPriorityQueueStateFactory,
    KeyedAggregatingStateFactory,
    KeyedReducingStateFactory,
)


_DRIVER: Optional[FSProcessorDriver] = None
_CONTEXT: Optional[WitContext] = None


def fs_exec(class_name: str, modules: List[Tuple[str, bytes]]) -> None:
    global _DRIVER

    try:
        # Load all modules in order
        loaded_modules = {}
        for module_name, module_bytes in modules:
            # Decode module bytes to string (assuming UTF-8 encoding)
            try:
                module_source = module_bytes.decode("utf-8")
            except UnicodeDecodeError:
                raise ValueError(
                    f"Failed to decode module_bytes as UTF-8 for module '{module_name}'"
                )

            # Create a module spec from the module name
            spec = importlib.util.spec_from_loader(module_name, loader=None)
            if spec is None:
                raise RuntimeError(f"Failed to create module spec for {module_name}")

            # Create the module
            module = importlib.util.module_from_spec(spec)

            # Execute the module source code.
            # exec is required: importlib.util.module_from_spec does not execute
            # code; Python's import system uses exec internally. This runtime
            # is designed to execute trusted user-provided processor code in
            # an isolated WASM sandbox. Only deploy code from trusted sources.
            code = compile(module_source, f"<{module_name}>", "exec")
            exec(code, module.__dict__)

            # Add the module to sys.modules
            sys.modules[module_name] = module
            loaded_modules[module_name] = module

        # Try to find the class in all loaded modules
        ProcessorClass = None

        # First, try to find in the last module (most likely location)
        if modules:
            last_module_name = modules[-1][0]
            if last_module_name in loaded_modules:
                module = loaded_modules[last_module_name]
                if hasattr(module, class_name):
                    ProcessorClass = getattr(module, class_name)

        # If not found, search in all modules
        if ProcessorClass is None:
            for module_name, module in loaded_modules.items():
                if hasattr(module, class_name):
                    ProcessorClass = getattr(module, class_name)
                    break

        if ProcessorClass is None:
            module_names = [name for name, _ in modules]
            raise RuntimeError(
                f"Class '{class_name}' not found in any of the loaded modules: {module_names}"
            )

        if not issubclass(ProcessorClass, FSProcessorDriver):
            raise TypeError(
                f"Class '{class_name}' must be a subclass of FSProcessorDriver"
            )

        _DRIVER = ProcessorClass()

    except Exception as e:
        raise RuntimeError(
            f"Failed to load class '{class_name}' from modules: {e}"
        ) from e


class WitWorld:

    def fs_init(self, config: List[Tuple[str, str]]) -> None:
        global _CONTEXT

        config_dict = convert_config_to_dict(config)

        _CONTEXT = WitContext(config_dict)

        if _DRIVER:
            try:
                _DRIVER.init(_CONTEXT, _CONTEXT._CONFIG)
            except Exception as e:
                logger.debug("Driver init failed (graceful degradation): %s", e)

    def fs_process(self, source_id: int, data: bytes) -> None:
        if not _DRIVER or not _CONTEXT:
            return

        try:
            _DRIVER.process(_CONTEXT, source_id, data)
        except Exception as e:
            logger.debug("Process error (graceful degradation): %s", e)

    def fs_process_watermark(self, source_id: int, watermark: int) -> None:
        if not _DRIVER or not _CONTEXT:
            return

        try:
            _DRIVER.process_watermark(_CONTEXT, source_id, watermark)
        except Exception as e:
            logger.debug("Watermark process error (graceful degradation): %s", e)

    def fs_take_checkpoint(self, checkpoint_id: int) -> None:
        if not _DRIVER or not _CONTEXT:
            return

        try:
            _DRIVER.take_checkpoint(_CONTEXT, checkpoint_id)
        except Exception as e:
            logger.debug("Checkpoint error (graceful degradation): %s", e)

    def fs_check_heartbeat(self) -> bool:
        if not _DRIVER or not _CONTEXT:
            return False

        try:
            return _DRIVER.check_heartbeat(_CONTEXT)
        except Exception as e:
            logger.debug("Heartbeat check failed (graceful degradation): %s", e)
            return False

    def fs_close(self) -> None:
        global _DRIVER, _CONTEXT

        if _DRIVER and _CONTEXT:
            try:
                _DRIVER.close(_CONTEXT)
            except Exception as e:
                logger.debug("Driver close error (cleanup continues): %s", e)

        _DRIVER = None
        _CONTEXT = None

    def fs_exec(self, class_name: str, modules: List[Tuple[str, bytes]]) -> None:
        fs_exec(class_name, modules)

    def fs_custom(self, payload: bytes) -> bytes:
        if not _DRIVER or not _CONTEXT:
            raise RuntimeError("Driver or Context not initialized")

        return _DRIVER.custom(payload)


__all__ = ['WitWorld']
