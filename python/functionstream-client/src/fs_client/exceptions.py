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
Custom exceptions for Function Stream Client.

This module maps gRPC status codes to semantic python exceptions,
allowing users to handle specific error cases (e.g., timeouts, auth failures)
granularly.
"""

from typing import Optional

import grpc


class FsError(Exception):
    """Base exception for all Function Stream errors."""
    def __init__(
        self, message: str, original_exception: Optional[Exception] = None
    ):
        super().__init__(message)
        self.original_exception = original_exception


class ClientError(FsError):
    """
    Raised when the error originates from the client side.
    Examples: File not found, invalid configuration, local validation failure.
    """


class ServerError(FsError):
    """
    Base class for errors returned by the server.
    This includes both business logic errors and gRPC protocol errors.
    """
    def __init__(
        self,
        message: str,
        status_code: Optional[int] = None,
        grpc_code: Optional[grpc.StatusCode] = None,
    ):
        super().__init__(message)
        # Business logic code (e.g., HTTP-like 400/500)
        self.status_code = status_code
        # gRPC protocol code (e.g., UNAVAILABLE)
        self.grpc_code = grpc_code


# --- Specific Network/Protocol Errors ---

class NetworkError(ServerError):
    """Raised when communication with the server fails (e.g., connection refused)."""


class FunctionStreamTimeoutError(ServerError):
    """Raised when the operation timed out."""


# --- Specific Business/Logic Errors (Mapped from gRPC Codes) ---

class BadRequestError(ServerError):
    """Raised when the arguments are invalid (INVALID_ARGUMENT)."""
    pass


class AuthenticationError(ServerError):
    """Raised when the client is not authenticated (UNAUTHENTICATED)."""
    pass


class PermissionDeniedError(ServerError):
    """Raised when the client does not have permission (PERMISSION_DENIED)."""
    pass


class NotFoundError(ServerError):
    """Raised when a requested resource is not found (NOT_FOUND)."""
    pass


class ConflictError(ServerError):
    """Raised when a resource already exists or state conflict occurs (ALREADY_EXISTS, ABORTED)."""
    pass


class ResourceExhaustedError(ServerError):
    """Raised when the server is out of resources or quota (RESOURCE_EXHAUSTED)."""
    pass


class InternalServerError(ServerError):
    """Raised when the server encountered an internal error (INTERNAL, DATA_LOSS, UNKNOWN)."""
    pass


# --- Mapping Logic ---

_GRPC_CODE_TO_EXCEPTION = {
    grpc.StatusCode.INVALID_ARGUMENT: BadRequestError,
    grpc.StatusCode.OUT_OF_RANGE: BadRequestError,

    grpc.StatusCode.UNAUTHENTICATED: AuthenticationError,
    grpc.StatusCode.PERMISSION_DENIED: PermissionDeniedError,

    grpc.StatusCode.NOT_FOUND: NotFoundError,

    grpc.StatusCode.ALREADY_EXISTS: ConflictError,
    grpc.StatusCode.ABORTED: ConflictError,

    grpc.StatusCode.RESOURCE_EXHAUSTED: ResourceExhaustedError,

    grpc.StatusCode.DEADLINE_EXCEEDED: FunctionStreamTimeoutError,
    grpc.StatusCode.UNAVAILABLE: NetworkError,

    grpc.StatusCode.INTERNAL: InternalServerError,
    grpc.StatusCode.DATA_LOSS: InternalServerError,
    grpc.StatusCode.UNKNOWN: InternalServerError,
    grpc.StatusCode.UNIMPLEMENTED: InternalServerError,
}


def _convert_grpc_error(e: grpc.RpcError) -> FsError:
    """
    Convert a gRPC RpcError into a semantic FsError subclass.
    """
    code = e.code()
    details = e.details() or "Unknown gRPC error"

    # 1. Look up the specific exception class
    exception_cls = _GRPC_CODE_TO_EXCEPTION.get(code, ServerError)

    # 2. Create the exception message
    message = f"{details} (gRPC code: {code.name if code else 'NONE'})"

    # 3. Return the instance (caller should raise it)
    return exception_cls(message, grpc_code=code)
