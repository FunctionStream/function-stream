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

import logging
import grpc
from pathlib import Path
from typing import Optional, Union, List, Dict, Type

from ._proto import function_stream_pb2, function_stream_pb2_grpc
from .exceptions import (
    ServerError, ClientError, _convert_grpc_error,
    BadRequestError, AuthenticationError, PermissionDeniedError,
    NotFoundError, ConflictError, ResourceExhaustedError, InternalServerError
)

logger = logging.getLogger(__name__)

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
            logger.debug(f"Initialized FsClient with existing channel to {self.target}")
        else:
            base_options = list(self.DEFAULT_OPTIONS)
            if options:
                base_options.extend(options)

            logger.info(f"Connecting to FunctionStream at {self.target} (secure={secure})...")

            if secure:
                creds = grpc.ssl_channel_credentials()
                self._channel = grpc.secure_channel(self.target, creds, options=base_options)
            else:
                self._channel = grpc.insecure_channel(self.target, options=base_options)

            self._own_channel = True

        self._stub = function_stream_pb2_grpc.FunctionStreamServiceStub(self._channel)

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

        logger.info(f"Creating function from files: wasm={f_path.name}, config={c_path.name}")

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
            raise TypeError(f"config_content must be str or bytes, got {type(config_content)}")

        logger.info("Creating function from in-memory bytes")
        return self._create_function_internal(function_bytes, real_conf_bytes, timeout)

    def _create_function_internal(self, func_bytes: bytes, conf_bytes: bytes, timeout: float) -> bool:
        request = function_stream_pb2.CreateFunctionRequest(
            function_bytes=func_bytes,
            config_bytes=conf_bytes,
        )
        self._invoke(self._stub.CreateFunction, request, timeout)
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
                logger.error(f"Server returned error: code={response.status_code}, msg={error_msg}")

                # Automatically map status code to specific exception
                exception_cls = self._STATUS_CODE_MAP.get(response.status_code, ServerError)
                raise exception_cls(message=error_msg, status_code=response.status_code)

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