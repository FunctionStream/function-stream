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
import json
from typing import Optional, Any

from ._proto import function_stream_pb2, function_stream_pb2_grpc
from .exceptions import ServerError, _convert_grpc_error


class FsClient:
    """
    High-level client for Function Stream gRPC service.
    
    This client wraps the generated gRPC stub and provides a convenient
    interface for interacting with the Function Stream service.
    
    Example:
        >>> with FsClient(host="localhost", port=8080) as client:
        ...     data = client.execute_sql("SHOW FUNCTIONS")
        ...     client.create_function_from_bytes(b"wasm_bytes...", b"config_bytes")
        ...     client.create_function_from_files("./app.wasm", "./config.yaml")
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 8080,
        secure: bool = False,
        channel: Optional[grpc.Channel] = None,
        **kwargs
    ):
        """
        Initialize the Function Stream client.

        Args:
            host: Server host address
            port: Server port
            secure: Whether to use TLS/SSL
            channel: Optional gRPC channel (if None, creates a new channel)
            **kwargs: Additional channel options (e.g., options for grpc.Channel)
        """
        self.target = f"{host}:{port}"
        
        if channel:
            self._channel = channel
            self._own_channel = False
        else:
            # Immediately initialize Channel to avoid lazy init complexity
            if secure:
                # Can be extended to load certificates
                creds = grpc.ssl_channel_credentials()
                self._channel = grpc.secure_channel(
                    self.target, creds, options=kwargs.get("options", [])
                )
            else:
                self._channel = grpc.insecure_channel(
                    self.target, options=kwargs.get("options", [])
                )
            self._own_channel = True

        self._stub = function_stream_pb2_grpc.FunctionStreamServiceStub(self._channel)

    def execute_sql(self, sql: str) -> Any:
        """
        Execute SQL and return the data directly.
        
        Args:
            sql: SQL statement to execute
            
        Returns:
            Parsed data (JSON if applicable, otherwise raw data)
            Returns None if no data field in response
            
        Raises:
            ServerError: If SQL execution fails (status_code >= 400)
            ClientError: For other client errors
        """
        request = function_stream_pb2.SqlRequest(sql=sql)
        return self._invoke(self._stub.ExecuteSql, request)

    def create_function_from_files(
        self,
        function_path: str,
        config_path: str,
    ) -> Any:
        """
        Create a function from file paths.
        
        Args:
            function_path: Path to WASM file (will be read as binary)
            config_path: Path to config file (will be read as binary)
            
        Returns:
            Parsed data (JSON if applicable, otherwise raw data)
            Returns None if no data field in response
            
        Raises:
            FileNotFoundError: If file does not exist
            ServerError: If function creation fails (status_code >= 400)
            ClientError: For other client errors
        """
        # Read WASM file as binary
        with open(function_path, "rb") as f:
            func_bytes = f.read()

        # Read config file as binary
        with open(config_path, "rb") as f:
            conf_bytes = f.read()

        request = function_stream_pb2.CreateFunctionRequest(
            function_bytes=func_bytes,
            config_bytes=conf_bytes,
        )
        
        return self._invoke(self._stub.CreateFunction, request)

    def create_function_from_bytes(
        self,
        function_bytes: bytes,
        config_bytes: str,
    ) -> Any:
        """
        Create a function from bytes.
        
        Args:
            function_bytes: WASM binary content
            config_bytes: Config YAML string content (will be encoded as UTF-8)
            
        Returns:
            Parsed data (JSON if applicable, otherwise raw data)
            Returns None if no data field in response
            
        Raises:
            ServerError: If function creation fails (status_code >= 400)
            ClientError: For other client errors
        """
        # Encode config YAML string as UTF-8 bytes
        conf_bytes = config_bytes.encode("utf-8")

        request = function_stream_pb2.CreateFunctionRequest(
            function_bytes=function_bytes,
            config_bytes=conf_bytes,
        )
        
        return self._invoke(self._stub.CreateFunction, request)

    def _invoke(self, func, request) -> Any:
        """
        Generic wrapper for gRPC calls to handle errors and parsing.
        
        Args:
            func: gRPC stub method to call
            request: Protobuf request message
            
        Returns:
            Parsed data (JSON if applicable, otherwise raw data)
            Returns None if no data field in response
            
        Raises:
            ServerError: If status_code >= 400
            ClientError: For other client errors
        """
        try:
            response = func(request)
            
            # Unified error checking
            if response.status_code >= 400:
                raise ServerError(
                    message=response.message,
                    status_code=response.status_code
                )
            
            # Success: return data directly (try JSON parsing)
            if response.HasField("data") and response.data:
                try:
                    return json.loads(response.data)
                except (json.JSONDecodeError, TypeError):
                    return response.data
            return None

        except grpc.RpcError as e:
            raise _convert_grpc_error(e)

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

