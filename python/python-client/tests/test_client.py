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
Tests for Function Stream Python Client
"""

import unittest
from unittest.mock import Mock, patch
from fs_client import FsClient, ServerError, ConnectionError


class TestFsClient(unittest.TestCase):
    """Test cases for FsClient"""

    def setUp(self):
        """Set up test fixtures"""
        self.client = FsClient(host="localhost", port=8080)

    def tearDown(self):
        """Clean up after tests"""
        if self.client:
            self.client.close()

    def test_client_initialization(self):
        """Test client initialization"""
        client = FsClient(host="localhost", port=8080)
        self.assertEqual(client.target, "localhost:8080")
        self.assertIsNotNone(client._stub)
        client.close()

    def test_client_context_manager(self):
        """Test client as context manager"""
        with FsClient(host="localhost", port=8080) as client:
            self.assertIsNotNone(client)
        # Channel should be closed after context exit

    def test_client_methods_exist(self):
        """Test that all expected methods exist"""
        methods = ["execute_sql", "create_function_from_files", "create_function_from_bytes", "close"]
        for method in methods:
            self.assertTrue(
                hasattr(self.client, method),
                f"Method {method} not found"
            )
            self.assertTrue(
                callable(getattr(self.client, method)),
                f"Method {method} is not callable"
            )

    def test_exceptions_import(self):
        """Test that exceptions can be imported"""
        from fs_client import (
            ClientError,
            ConnectionError,
            ServerError,
        )
        self.assertTrue(issubclass(ConnectionError, ClientError))
        self.assertTrue(issubclass(ServerError, ClientError))


if __name__ == "__main__":
    unittest.main()
