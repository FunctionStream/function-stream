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
Stateless utility functions: resilient port allocation and health checks.
"""

import logging
import socket
import time
from typing import Optional

logger = logging.getLogger(__name__)


class NetworkUtilityError(Exception):
    """Base exception for network utility functions."""
    pass


class PortAllocationError(NetworkUtilityError):
    """Raised when a free port cannot be allocated."""
    pass


def find_free_port(host: str = "127.0.0.1", port_range: Optional[tuple[int, int]] = None) -> int:
    """
    Finds a reliable, available TCP port.

    By default, relies on the OS kernel to allocate an ephemeral port (binding to port 0).
    This guarantees no immediate collision and is extremely fast.

    Args:
        host: The interface IP to bind against.
        port_range: Optional tuple of (start, end). If provided, falls back to scanning
                    this specific range instead of OS assignment.

    Raises:
        PortAllocationError: If binding fails or no ports are available in the range.
    """
    if port_range is not None:
        start, end = port_range
        import random
        candidates = list(range(start, end + 1))
        random.shuffle(candidates)

        for port in candidates:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind((host, port))
                    return port
                except OSError:
                    continue

        raise PortAllocationError(f"Exhausted all ports in requested range [{start}, {end}].")

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            s.bind((host, 0))
            allocated_port = s.getsockname()[1]
            return allocated_port
        except OSError as e:
            raise PortAllocationError(f"OS failed to allocate an ephemeral port on {host}: {e}") from e


def wait_for_port(
        host: str,
        port: int,
        timeout: float = 30.0,
        poll_interval: float = 0.5
) -> bool:
    """
    Blocks until the given TCP port is accepting connections.

    Uses `socket.create_connection` for robust DNS resolution and
    transparent IPv4/IPv6 dual-stack support.

    Args:
        host: Hostname or IP address.
        port: Target TCP port.
        timeout: Max seconds to wait.
        poll_interval: Seconds to wait between retries.

    Returns:
        True if connection succeeded within timeout, False otherwise.
    """
    logger.debug("Waiting up to %.1fs for %s:%d to become responsive...", timeout, host, port)
    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:
        try:
            with socket.create_connection((host, port), timeout=poll_interval):
                logger.debug("Successfully connected to %s:%d", host, port)
                return True
        except (OSError, ConnectionRefusedError):
            pass

        time.sleep(poll_interval)

    logger.error("Timeout reached: %s:%d did not accept connections after %.1fs.", host, port, timeout)
    return False