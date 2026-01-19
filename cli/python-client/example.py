#!/usr/bin/env python3
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
Example usage of Function Stream Python Client

Before running this script, make sure to:
1. Install the package: pip install -e ".[dev]"
2. Generate proto code: make proto
"""

try:
    from fs_client import FsClient, ServerError
except ImportError as e:
    print("Error: fs_client module not found.")
    print("Please install the package first:")
    print("  pip install -e \".[dev]\"")
    print("  make proto")
    print(f"\nOriginal error: {e}")
    exit(1)


def main():
    """Example usage of the Function Stream client"""
    # Create a client using context manager
    with FsClient(host="localhost", port=8080) as client:
        try:
            # Execute SQL
            print("Executing SQL...")
            response = client.execute_sql("SHOW WASMTASKS")
            print(f"Status: {response['status_code']}")
            print(f"Message: {response['message']}")

            # Create function
            print("\nCreating function...")
            response = client.create_function(
                config_path="/path/to/config.yaml",
                wasm_path="/path/to/module.wasm"
            )
            print(f"Status: {response['status_code']}")
            print(f"Message: {response['message']}")

        except ServerError as e:
            print(f"Server error (status {e.status_code}): {e}")
        except Exception as e:
            print(f"Error: {e}")


if __name__ == "__main__":
    main()
