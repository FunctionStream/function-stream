# Function Stream Python Client

High-level Python gRPC client library for the Function Stream service.

## Project Structure

```
python-client/
├── pyproject.toml              # 项目配置
├── protos/                      # Proto 文件目录
│   └── function_stream.proto
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
from fs_client import FsClient

with FsClient(host="localhost", port=8080) as client:
    # Execute SQL
    response = client.execute_sql("SHOW WASMTASKS")
    print(response["message"])
    
    # Create function
    response = client.create_function(
        config_path="/path/to/config.yaml",
        wasm_path="/path/to/module.wasm"
    )
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

### `FsClient(host="localhost", port=8080, secure=False, ...)`

Initialize the Function Stream client.

**Parameters:**
- `host`: Server host address (default: "localhost")
- `port`: Server port (default: 8080)
- `secure`: Whether to use TLS/SSL (default: False)
- `channel`: Optional gRPC channel
- `credentials`: Optional channel credentials
- `options`: Optional list of channel options

### `execute_sql(sql)`

Execute a SQL statement.

**Returns:** Dictionary with `status_code`, `message`, and optional `data`

**Raises:** `ServerError` if SQL execution fails (status_code >= 400)

### `create_function(config_path, wasm_path)`

Create a function from config and WASM file paths.

The paths are sent as UTF-8 encoded bytes to the server.

**Returns:** Dictionary with `status_code`, `message`, and optional `data`

**Raises:** `ServerError` if function creation fails (status_code >= 400)

### `close()`

Close the gRPC channel (if owned by this client).

## Exception Handling

The client provides custom exceptions for better error handling:

```python
from fs_client import FsClient, AuthenticationError, ServerError, ConnectionError

try:
    client = FsClient("localhost", 8080)
    response = client.execute_sql("SHOW WASMTASKS")
except ServerError as e:
    print(f"Server error: {e}")
except ConnectionError:
    print("Cannot connect to server")
except ServerError as e:
    print(f"Server error (status {e.status_code}): {e}")
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
