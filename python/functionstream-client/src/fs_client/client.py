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

"""
Function Stream gRPC Client

This module provides a production-ready client for the Function Stream service.
It handles connection management, error translation, logging, and type safety.
"""

import ast
import importlib.util
import inspect
import logging
import sys
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Type, Union

import grpc

from fs_api import FSProcessorDriver

from ._proto import function_stream_pb2, function_stream_pb2_grpc
from .config import WasmTaskConfig
from .exceptions import (
    AuthenticationError,
    BadRequestError,
    ClientError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    PermissionDeniedError,
    ResourceExhaustedError,
    ServerError,
    _convert_grpc_error,
)
from .models import FunctionInfo, ShowFunctionsResult

logger = logging.getLogger(__name__)


def _resolve_relative(
    module_part: Optional[str], level: int, current_package: str
) -> str:
    if level == 0:
        return module_part or ""
    parts = (current_package or "").split(".")
    for _ in range(level - 1):
        if parts:
            parts.pop()
    prefix = ".".join(parts)
    if module_part:
        return f"{prefix}.{module_part}" if prefix else module_part
    return prefix


def _get_imported_names(tree: ast.AST) -> List[Tuple[Optional[str], int]]:
    out: List[Tuple[Optional[str], int]] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                out.append((alias.name, 0))
        elif isinstance(node, ast.ImportFrom):
            out.append((node.module, node.level or 0))
    return out


def _is_under_site_or_stdlib(origin: Path) -> bool:
    try:
        resolved = origin.resolve()
        parts = resolved.parts
        if "site-packages" in parts or "dist-packages" in parts:
            return True
        base = Path(sys.base_prefix)
        try:
            resolved.relative_to(base / "lib")
            return True
        except ValueError:
            return False
    except (OSError, RuntimeError):
        return True


def _get_module_origin(
    module_name: str,
    result: Dict[str, Path],
) -> Optional[Path]:
    """Resolve module origin path from spec or existing result."""
    if module_name == "__main__" and module_name in result:
        return result[module_name]
    try:
        spec = importlib.util.find_spec(module_name)
    except (ImportError, ValueError, ModuleNotFoundError):
        return None
    if spec is None or spec.origin is None or spec.origin == "built-in":
        return None
    return Path(spec.origin)


def _process_module_deps(
    module_name: str,
    origin: Path,
    package: str,
    dep_graph: Dict[str, Set[str]],
    seen: Set[str],
    queue: List[str],
) -> None:
    """Parse module and add its imports to dep_graph and queue."""
    try:
        tree = ast.parse(origin.read_text(encoding="utf-8"))
    except (OSError, SyntaxError):
        return
    for module_part, level in _get_imported_names(tree):
        abs_name = (
            module_part
            if level == 0
            else _resolve_relative(module_part, level, package)
        )
        if abs_name:
            dep_graph[module_name].add(abs_name)
            if abs_name not in seen:
                queue.append(abs_name)


def _collect_local_deps(
    driver_class: Type,
    driver_file: Path,
) -> Tuple[Dict[str, Path], Dict[str, Set[str]]]:
    driver_root = driver_file.resolve().parent
    driver_module_name = driver_class.__module__
    result: Dict[str, Path] = {}
    dep_graph: Dict[str, Set[str]] = {}
    queue: List[str] = [driver_module_name]
    seen: Set[str] = set()

    if driver_module_name == "__main__":
        result[driver_module_name] = driver_file.resolve()
        dep_graph[driver_module_name] = set()

    while queue:
        module_name = queue.pop(0)
        if module_name in seen:
            continue
        seen.add(module_name)
        package = module_name.rpartition(".")[0]

        origin = _get_module_origin(module_name, result)
        if origin is None:
            continue
        if _is_under_site_or_stdlib(origin):
            continue
        try:
            origin.resolve().relative_to(driver_root)
        except ValueError:
            continue

        result[module_name] = origin
        dep_graph[module_name] = set()
        _process_module_deps(
            module_name, origin, package, dep_graph, seen, queue
        )

    return result, dep_graph


def _topo_order(nodes: List[str], graph: Dict[str, Set[str]]) -> List[str]:
    node_set = set(nodes)
    order: List[str] = []
    g = {n: graph.get(n, set()) & node_set for n in nodes}
    while g:
        for n in list(g):
            if all(d in order for d in g[n]):
                order.append(n)
                del g[n]
                break
        else:
            break
    for n in nodes:
        if n not in order:
            order.append(n)
    return order


class FsClient:
    """
    High-level, thread-safe client for Function Stream gRPC service.
    """

    DEFAULT_TIMEOUT = 30.0

    DEFAULT_OPTIONS = [
        ('grpc.keepalive_time_ms', 10000),
        ('grpc.keepalive_timeout_ms', 5000),
        ('grpc.keepalive_permit_without_calls', 1),
        ('grpc.http2.max_pings_without_data', 0),
    ]

    # Mapping Business Status Codes (HTTP-like) to python Exceptions
    _STATUS_CODE_MAP: Dict[int, Type[ServerError]] = {
        400: BadRequestError,
        401: AuthenticationError,
        403: PermissionDeniedError,
        404: NotFoundError,
        409: ConflictError,
        429: ResourceExhaustedError,
        500: InternalServerError,
    }

    def __init__(
            self,
            host: str = "localhost",
            port: int = 8080,
            secure: bool = False,
            channel: Optional[grpc.Channel] = None,
            options: Optional[List[tuple]] = None,
            default_timeout: float = DEFAULT_TIMEOUT,
    ):
        self.target = f"{host}:{port}"
        self.default_timeout = default_timeout
        self._own_channel = False

        if channel:
            self._channel = channel
            logger.debug(
                f"Initialized FsClient with existing channel to {self.target}"
            )
        else:
            base_options = list(self.DEFAULT_OPTIONS)
            if options:
                base_options.extend(options)

            logger.info(
                f"Connecting to FunctionStream at {self.target} (secure={secure})..."
            )

            if secure:
                creds = grpc.ssl_channel_credentials()
                self._channel = grpc.secure_channel(
                    self.target, creds, options=base_options
                )
            else:
                self._channel = grpc.insecure_channel(
                    self.target, options=base_options
                )

            self._own_channel = True

        self._stub = function_stream_pb2_grpc.FunctionStreamServiceStub(
            self._channel
        )

    def create_function_from_files(
            self,
            function_path: Union[str, Path],
            config_path: Union[str, Path],
            timeout: Optional[float] = None,
    ) -> bool:
        """
        Create a function by reading local files.
        """
        f_path = Path(function_path)
        c_path = Path(config_path)

        if not f_path.exists():
            raise FileNotFoundError(f"WASM file not found: {f_path}")
        if not c_path.exists():
            raise FileNotFoundError(f"Config file not found: {c_path}")

        logger.info(
            f"Creating function from files: wasm={f_path.name}, config={c_path.name}"
        )

        try:
            func_bytes = f_path.read_bytes()
            conf_bytes = c_path.read_bytes()
        except OSError as e:
            logger.error(f"Failed to read function files: {e}")
            raise ClientError(f"File access error: {e}") from e

        return self._create_function_internal(func_bytes, conf_bytes, timeout)

    def create_function_from_bytes(
            self,
            function_bytes: bytes,
            config_content: Union[str, bytes],
            timeout: Optional[float] = None,
    ) -> bool:
        """
        Create a function from in-memory bytes/strings.
        """
        if isinstance(config_content, str):
            real_conf_bytes = config_content.encode("utf-8")
        elif isinstance(config_content, bytes):
            real_conf_bytes = config_content
        else:
            raise TypeError(
                f"config_content must be str or bytes, got {type(config_content)}"
            )

        logger.info("Creating function from in-memory bytes")
        return self._create_function_internal(
            function_bytes, real_conf_bytes, timeout
        )

    def _create_function_internal(
        self, func_bytes: bytes, conf_bytes: bytes, timeout: float
    ) -> bool:
        request = function_stream_pb2.CreateFunctionRequest(
            function_bytes=func_bytes,
            config_bytes=conf_bytes,
        )
        self._invoke(self._stub.CreateFunction, request, timeout)
        return True

    def create_python_function(
            self,
            class_name: str,
            modules: List[tuple[str, bytes]],
            config_content: str,
    ) -> bool:
        """
        Create Python function dynamically by loading Python modules.

        Args:
            class_name: Name of the Python class to load and instantiate
            modules: List of tuples (module_name, module_bytes) containing
                Python module code
            config_content: Configuration content as string

        Returns:
            True if successful

        Raises:
            ClientError: If the request fails
        """
        if not modules:
            raise ValueError("At least one module is required")

        logger.info(
            f"Creating Python function: class_name='{class_name}', "
            f"modules={len(modules)}"
        )

        # Convert modules to proto format
        proto_modules = [
            function_stream_pb2.PythonModule(
                module_name=module_name,
                module_bytes=module_bytes
            )
            for module_name, module_bytes in modules
        ]

        request = function_stream_pb2.CreatePythonFunctionRequest(
            class_name=class_name,
            modules=proto_modules,
            config_content=config_content,
        )

        self._invoke(self._stub.CreatePythonFunction, request, None)
        return True

    def create_python_function_from_config(
            self,
            config: WasmTaskConfig,
            driver_class: Type,
    ) -> bool:
        """
        Create Python function from Config and Driver implementation.

        Extracts class_name and module bytes from the Driver class
        automatically,
        and uses the Config's to_yaml() for config_content.

        Args:
            config: WasmTaskConfig
            driver_class: The FSProcessorDriver subclass implementing the
                processor

        Returns:
            True if successful

        Raises:
            ValueError: If driver_class is not a class
            TypeError: If driver_class does not implement FSProcessorDriver
            ClientError: If the request fails
        """
        if not inspect.isclass(driver_class):
            raise ValueError("driver_class must be a class, not an instance")

        if not issubclass(driver_class, FSProcessorDriver):
            raise TypeError(
                f"driver_class must implement FSProcessorDriver, "
                f"got {driver_class.__name__}"
            )

        class_name = driver_class.__name__
        driver_file = Path(inspect.getfile(driver_class))
        paths, dep_graph = _collect_local_deps(driver_class, driver_file)
        order = _topo_order(list(paths.keys()), dep_graph)
        modules: List[Tuple[str, bytes]] = []
        for name in order:
            display_name = driver_file.stem if name == "__main__" else name
            try:
                modules.append((display_name, paths[name].read_bytes()))
            except OSError as e:
                raise ClientError(
                    f"Failed to read module: {paths[name]}"
                ) from e

        config_content = config.to_yaml()

        logger.info(
            f"Creating Python function from config: class_name='{class_name}', "
            f"config_name='{getattr(config, 'task_name', 'unknown')}', "
            f"modules={len(modules)}"
        )

        return self.create_python_function(
            class_name=class_name,
            modules=modules,
            config_content=config_content,
        )

    def drop_function(
            self,
            function_name: str,
            timeout: Optional[float] = None,
    ) -> bool:
        """
        Drop a function by name.

        Args:
            function_name: Name of the function to drop.
            timeout: Optional timeout in seconds.

        Returns:
            True if successful.
        """
        request = function_stream_pb2.DropFunctionRequest(function_name=function_name)
        self._invoke(self._stub.DropFunction, request, timeout)
        return True

    def show_functions(
            self,
            filter_pattern: Optional[str] = None,
            timeout: Optional[float] = None,
    ) -> ShowFunctionsResult:
        """
        List functions, optionally filtered.

        Args:
            filter_pattern: Optional filter string; None to list all.
            timeout: Optional timeout in seconds.

        Returns:
            ShowFunctionsResult with status_code, message, and list of FunctionInfo.
        """
        req = function_stream_pb2.ShowFunctionsRequest()
        if filter_pattern is not None:
            req.filter = filter_pattern
        actual_timeout = timeout if timeout is not None else self.default_timeout
        try:
            response = self._stub.ShowFunctions(req, timeout=actual_timeout)
        except grpc.RpcError as e:
            logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise _convert_grpc_error(e) from e

        code = response.status_code
        if code >= 400:
            error_msg = response.message or "Unknown server error"
            logger.error(f"Server returned error: code={code}, msg={error_msg}")
            exception_cls = self._STATUS_CODE_MAP.get(code, ServerError)
            raise exception_cls(message=error_msg, status_code=code)

        functions = [
            FunctionInfo(name=f.name, task_type=f.task_type, status=f.status)
            for f in response.functions
        ]
        return ShowFunctionsResult(
            status_code=code,
            message=response.message or "",
            functions=functions,
        )

    def start_function(
            self,
            function_name: str,
            timeout: Optional[float] = None,
    ) -> bool:
        """
        Start a function by name.

        Args:
            function_name: Name of the function to start.
            timeout: Optional timeout in seconds.

        Returns:
            True if successful.
        """
        request = function_stream_pb2.StartFunctionRequest(function_name=function_name)
        self._invoke(self._stub.StartFunction, request, timeout)
        return True

    def stop_function(
            self,
            function_name: str,
            timeout: Optional[float] = None,
    ) -> bool:
        """
        Stop a function by name.

        Args:
            function_name: Name of the function to stop.
            timeout: Optional timeout in seconds.

        Returns:
            True if successful.
        """
        request = function_stream_pb2.StopFunctionRequest(function_name=function_name)
        self._invoke(self._stub.StopFunction, request, timeout)
        return True

    def _invoke(self, rpc_method, request, timeout: Optional[float]):
        """
        Generic gRPC invocation wrapper with Error Mapping.
        """
        actual_timeout = timeout if timeout is not None else self.default_timeout

        try:
            response = rpc_method(request, timeout=actual_timeout)

            if response.status_code >= 400:
                error_msg = response.message or "Unknown server error"
                logger.error(
                    f"Server returned error: code={response.status_code}, "
                    f"msg={error_msg}"
                )

                # Automatically map status code to specific exception
                exception_cls = self._STATUS_CODE_MAP.get(
                    response.status_code, ServerError
                )
                raise exception_cls(
                    message=error_msg, status_code=response.status_code
                )

            return response

        except grpc.RpcError as e:
            logger.error(f"gRPC call failed: {e.code()} - {e.details()}")
            raise _convert_grpc_error(e) from e

    def close(self):
        if self._own_channel and self._channel:
            logger.debug("Closing gRPC channel")
            self._channel.close()
            self._channel = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
