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
Function Stream gRPC Client

High-level wrapper around the generated gRPC stub.
"""

import grpc
from typing import Optional, Dict, Any
import json

from ._proto import function_stream_pb2, function_stream_pb2_grpc
from .exceptions import ClientError, ServerError, AuthenticationError, _convert_grpc_error


class FsClient:
    """
    High-level client for Function Stream gRPC service.
    
    This client wraps the generated gRPC stub and provides a convenient
    interface for interacting with the Function Stream service.
    
    Example:
        >>> with FsClient(host="localhost", port=8080) as client:
        ...     client.execute_sql("SHOW WASMTASKS")
        ...     client.create_function("/path/to/config.yaml", "/path/to/module.wasm")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        secure: bool = False,
        channel: Optional[grpc.Channel] = None,
        credentials: Optional[grpc.ChannelCredentials] = None,
        options: Optional[list] = None,
    ):
        """
        Initialize the Function Stream client.

        Args:
            host: Server host address
            port: Server port
            secure: Whether to use TLS/SSL
            channel: Optional gRPC channel (if None, creates a new channel)
            credentials: Optional channel credentials for secure connections
            options: Optional list of channel options
        """
        self.host = host
        self.port = port
        self.target = f"{host}:{port}"
        self._channel = channel
        self._credentials = credentials
        self._options = options or []
        self._stub = None
        self._own_channel = False

    def _ensure_stub(self):
        """Ensure the gRPC stub is initialized."""
        if self._stub is None:
            if self._channel is None:
                if self._credentials:
                    self._channel = grpc.secure_channel(
                        self.target, self._credentials, options=self._options
                    )
                else:
                    self._channel = grpc.insecure_channel(
                        self.target, options=self._options
                    )
                self._own_channel = True

            self._stub = function_stream_pb2_grpc.FunctionStreamServiceStub(
                self._channel
            )

    def execute_sql(self, sql: str) -> Dict[str, Any]:
        """
        Execute a SQL statement.

        Args:
            sql: SQL statement to execute

        Returns:
            Dictionary containing status_code, message, and optional data

        Raises:
            ServerError: If SQL execution fails
            ClientError: For other client errors
        """
        self._ensure_stub()

        request = function_stream_pb2.SqlRequest(sql=sql)

        try:
            response = self._stub.ExecuteSql(request)
            result = self._response_to_dict(response)
            
            # Check status code and raise exception if error
            if result["status_code"] >= 400:
                raise ServerError(
                    result.get("message", "SQL execution failed"),
                    status_code=result["status_code"]
                )
            
            return result
        except grpc.RpcError as e:
            raise _convert_grpc_error(e)

    def create_function(
        self, config_path: str, wasm_path: str
    ) -> Dict[str, Any]:
        """
        Create a function from config and WASM file paths.

        Args:
            config_path: Path to configuration file (will be sent as UTF-8 bytes)
            wasm_path: Path to WASM file (will be sent as UTF-8 bytes)

        Returns:
            Dictionary containing status_code, message, and optional data

        Raises:
            ServerError: If function creation fails
            ClientError: For other client errors
        """
        self._ensure_stub()

        # Convert paths to UTF-8 bytes as expected by the server
        config_bytes = config_path.encode("utf-8")
        wasm_bytes = wasm_path.encode("utf-8")

        request = function_stream_pb2.CreateFunctionRequest(
            config_bytes=config_bytes,
            wasm_bytes=wasm_bytes,
        )

        try:
            response = self._stub.CreateFunction(request)
            result = self._response_to_dict(response)
            
            # Check status code and raise exception if error
            if result["status_code"] >= 400:
                raise ServerError(
                    result.get("message", "Function creation failed"),
                    status_code=result["status_code"]
                )
            
            return result
        except grpc.RpcError as e:
            raise _convert_grpc_error(e)

    def _response_to_dict(self, response) -> Dict[str, Any]:
        """Convert protobuf Response to dictionary."""
        result = {
            "status_code": response.status_code,
            "message": response.message,
        }
        if response.HasField("data") and response.data:
            try:
                result["data"] = json.loads(response.data)
            except json.JSONDecodeError:
                result["data"] = response.data
        return result

    def close(self):
        """Close the gRPC channel (if we own it)."""
        if self._own_channel and self._channel is not None:
            self._channel.close()
            self._channel = None
            self._stub = None

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()

