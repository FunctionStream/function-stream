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

from typing import TYPE_CHECKING

from fs_api.store.error import (
    KvError,
    KvNotFoundError,
    KvIOError,
    KvOtherError,
)

if TYPE_CHECKING:
    from wit_world.imports.kv import (
        Error_NotFound,
        Error_IoError,
        Error_Other,
    )
else:
    try:
        from wit_world.imports.kv import (
            Error_NotFound,
            Error_IoError,
            Error_Other,
        )
    except (ImportError, AttributeError):
        Error_NotFound = None
        Error_IoError = None
        Error_Other = None


def wit_to_api_error(wit_error) -> KvError:
    if Error_NotFound is not None and isinstance(wit_error, Error_NotFound):
        return KvNotFoundError()
    elif Error_IoError is not None and isinstance(wit_error, Error_IoError):
        return KvIOError(wit_error.value)
    elif Error_Other is not None and isinstance(wit_error, Error_Other):
        return KvOtherError(wit_error.value)
    
    if wit_error == "not-found" or (isinstance(wit_error, str) and wit_error == "not-found"):
        return KvNotFoundError()
    elif isinstance(wit_error, tuple) and len(wit_error) > 0:
        if wit_error[0] == "io-error":
            message = wit_error[1] if len(wit_error) > 1 else "IO error"
            return KvIOError(message)
        elif wit_error[0] == "other":
            message = wit_error[1] if len(wit_error) > 1 else "Other error"
            return KvOtherError(message)
    
    return KvOtherError("Unknown error")


def api_to_wit_error(py_error: KvError):
    if Error_NotFound is not None and Error_IoError is not None and Error_Other is not None:
        if isinstance(py_error, KvNotFoundError):
            return Error_NotFound()
        elif isinstance(py_error, KvIOError):
            message = py_error.message if hasattr(py_error, 'message') else str(py_error)
            return Error_IoError(value=message)
        elif isinstance(py_error, KvOtherError):
            message = py_error.message if hasattr(py_error, 'message') else str(py_error)
            return Error_Other(value=message)
        else:
            return Error_Other(value=str(py_error))
    else:
        if isinstance(py_error, KvNotFoundError):
            return "not-found"
        elif isinstance(py_error, KvIOError):
            message = py_error.message if hasattr(py_error, 'message') else "IO error"
            return ("io-error", message)
        elif isinstance(py_error, KvOtherError):
            message = py_error.message if hasattr(py_error, 'message') else "Other error"
            return ("other", message)
        else:
            return ("other", str(py_error))


__all__ = ['wit_to_api_error', 'api_to_wit_error']

