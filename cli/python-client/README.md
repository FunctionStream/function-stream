# Function Stream Python Client

High-level Python gRPC client library for the Function Stream service.

## Project Structure

```
python-client/
├── pyproject.toml              # 项目配置
├── scripts/                     # 代码生成脚本
│   └── codegen.py
└── src/
    └── fs_client/               # 主包
        ├── __init__.py          # 导出 FsClient 和异常类
        ├── client.py             # Wrapper 类（用户接口）
        ├── exceptions.py         # 自定义异常
        ├── py.typed              # 类型提示标记
        └── _proto/               # 生成的代码（内部实现）
            ├── __init__.py
            ├── function_stream_pb2.py
            └── function_stream_pb2_grpc.py
```

**Note:** Proto file is located at `../../protocol/proto/function_stream.proto` (shared with the main project).

## Quick Start

### 1. Install Dependencies

```bash
# Install the package with dev dependencies (includes grpcio-tools, mypy-protobuf)
pip install -e ".[dev]"
```

### 2. Generate Proto Code

```bash
# Using Makefile
make proto

# Or directly
python3 scripts/codegen.py
```

This will:
- Generate Python code from `.proto` files
- Generate type hint files (`.pyi`) if `mypy-protobuf` is installed
- Fix import issues automatically

### 3. Use the Client

```python
from fs_client import FsClient, ServerError

with FsClient(host="localhost", port=8080) as client:
    try:
        # Execute SQL - returns data directly
        data = client.execute_sql("SHOW FUNCTIONS")
        print(f"Data: {data}")

        # Create function from file paths
        data = client.create_function_from_files(
            function_path="./app.wasm",
            config_path="./config.yaml"
        )
        print(f"Result: {data}")

        # Create function from bytes
        wasm_bytes = b"wasm binary content..."
        config_str = "name: my_function\ntype: python"
        data = client.create_function_from_bytes(
            function_bytes=wasm_bytes,
            config_bytes=config_str
        )
        print(f"Result: {data}")

    except ServerError as e:
        print(f"Server error (status {e.status_code}): {e}")
    except Exception as e:
        print(f"Error: {e}")
```

## Installation

### Using Virtual Environment

```bash
# Activate virtual environment
source .venv/bin/activate  # On Unix/Mac
# or
.venv\Scripts\activate     # On Windows

# Install package
pip install -e ".[dev]"

# Generate proto code
make proto
```

## API Reference

### `FsClient(host="localhost", port=8080, secure=False, channel=None, **kwargs)`

Initialize the Function Stream client.

**Parameters:**
- `host`: Server host address (default: "localhost")
- `port`: Server port (default: 8080)
- `secure`: Whether to use TLS/SSL (default: False)
- `channel`: Optional gRPC channel (if None, creates a new channel)
- `**kwargs`: Additional channel options (e.g., `options` for grpc.Channel)

### `execute_sql(sql)`

Execute a SQL statement.

**Parameters:**
- `sql`: SQL statement to execute

**Returns:** Parsed data (JSON if applicable, otherwise raw data). Returns `None` if no data field in response.

**Raises:** `ServerError` if SQL execution fails (status_code >= 400)

### `create_function_from_files(function_path, config_path)`

Create a function from file paths.

**Parameters:**
- `function_path`: Path to WASM file (will be read as binary)
- `config_path`: Path to config file (will be read as binary)

**Returns:** Parsed data (JSON if applicable, otherwise raw data). Returns `None` if no data field in response.

**Raises:**
- `FileNotFoundError`: If file does not exist
- `ServerError`: If function creation fails (status_code >= 400)

### `create_function_from_bytes(function_bytes, config_bytes)`

Create a function from bytes.

**Parameters:**
- `function_bytes`: WASM binary content
- `config_bytes`: Config YAML string content (will be encoded as UTF-8)

**Returns:** Parsed data (JSON if applicable, otherwise raw data). Returns `None` if no data field in response.

**Raises:**
- `ServerError`: If function creation fails (status_code >= 400)

### `close()`

Close the gRPC channel (if owned by this client).

## Exception Handling

The client provides custom exceptions for better error handling:

```python
from fs_client import FsClient, ServerError, ConnectionError, ClientError

try:
    client = FsClient("localhost", 8080)
    data = client.execute_sql("SHOW FUNCTIONS")
    print(f"Result: {data}")
except ConnectionError:
    print("Failed to connect to server")
except ServerError as e:
    print(f"Server error (status {e.status_code}): {e}")
except ClientError as e:
    print(f"Client error: {e}")
except Exception as e:
    print(f"An unexpected error occurred: {e}")
```

## Development

### Regenerating Proto Code

After modifying `.proto` files:

```bash
make proto
```

Or:

```bash
python3 scripts/codegen.py
```

### Running Tests

```bash
make test
```

### Cleaning Generated Files

```bash
make clean
```

## Type Hints

To enable type hints in your IDE, install `mypy-protobuf`:

```bash
pip install mypy-protobuf
```

Then regenerate proto code:

```bash
make proto
```

This will generate `.pyi` files alongside the generated Python code.
