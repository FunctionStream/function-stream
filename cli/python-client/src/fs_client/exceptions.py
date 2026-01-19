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
Custom exceptions for Function Stream Client
"""

import grpc


class ClientError(Exception):
    """Base exception for all client errors"""
    pass


class ConnectionError(ClientError):
    """Raised when connection to server fails"""
    pass


class AuthenticationError(ClientError):
    """Raised when authentication fails"""
    pass


class ServerError(ClientError):
    """Raised when server returns an error"""
    def __init__(self, message: str, status_code: int = None):
        super().__init__(message)
        self.status_code = status_code


def _convert_grpc_error(e: grpc.RpcError) -> ClientError:
    """
    Convert gRPC error to appropriate client exception.
    
    Args:
        e: gRPC RpcError
        
    Returns:
        Appropriate ClientError subclass
    """
    code = e.code()
    details = e.details()
    
    if code == grpc.StatusCode.UNAUTHENTICATED:
        return AuthenticationError(f"Authentication failed: {details}")
    elif code == grpc.StatusCode.UNAVAILABLE:
        return ConnectionError(f"Connection failed: {details}")
    elif code == grpc.StatusCode.INTERNAL:
        return ServerError(f"Server error: {details}", status_code=500)
    else:
        return ClientError(f"RPC error ({code}): {details}")

