# Function Management and Development Guide

Function is the core computational unit of Function Stream. This guide takes Go (WASM) mode as an example to detail the full lifecycle management of a Function and compares the operational differences with Python mode.

---

## 1. Function Type Overview

Before starting, please choose the appropriate operator type according to your business scenario:

| Feature             | WASM Function (type: processor)                                     | Python Function (type: python)                                       |
|---------------------|---------------------------------------------------------------------|----------------------------------------------------------------------|
| Supported Languages | Go, Rust, C++, Python (Compiled)                                    | Native Python Script                                                 |
| Performance         | Extremely High (Near native, suitable for high throughput cleaning) | Medium (High development efficiency, suitable for algorithmic logic) |
| Isolation           | Strong Sandbox Isolation                                            | Process-level Isolation                                              |
| Registration Method | SQL CLI / Python Client / gRPC                                      | Python Client (gRPC) Only                                            |

---

## 2. Operator Development Example: Go (WASM)

### 2.1 Build Process

Function Stream adopts the WebAssembly Component Model. Go code needs to be compiled into a WASM binary that satisfies the interface definitions.

```bash
# Enter the example directory and execute automated build
cd examples/go-processor
chmod +x build.sh
./build.sh
```

- **Artifact**: `build/processor.wasm`
- **Dependencies**: Ensure TinyGo, wasm-tools, and wit-bindgen-go are installed in the environment.

### 2.2 Task Topology Configuration (config.yaml)

This file defines the data flow and runtime attributes of the function. See [Function Task Configuration Specification](function-configuration.md) for full field descriptions.

```yaml
name: "go-processor-example"   # Global unique identifier for the task
type: processor                # Specify as WASM mode

input-groups:                  # Define input source topology
  - inputs:
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic"
        group_id: "go-processor-group"

outputs:                       # Define output channels
  - output-type: kafka
    bootstrap_servers: "localhost:9092"
    topic: "output-topic"
    partition: 0
```

---

## 3. Registration and Deployment Management

### 3.1 Method 1: Use SQL CLI Management (Recommended for Operations)

Suitable for manual deployment and quick troubleshooting in production.

```sql
-- 1. Register function (Must use absolute path)
CREATE FUNCTION WITH (
  'function_path'='/opt/fs/examples/go-processor/build/processor.wasm',
  'config_path'='/opt/fs/examples/go-processor/config.yaml'
);

-- 2. Start processing task
START FUNCTION go-processor-example;

-- 3. View running status
SHOW FUNCTIONS;
```

### 3.2 Method 2: Use Python SDK Management (Recommended for Automation)

Suitable for CI/CD pipelines or writing automated scheduling scripts.

```python
from fs_client import FsClient

# Securely connect to the server via context manager
with FsClient(host="localhost", port=8080) as client:
    # Register WASM function
    client.create_function_from_files(
        function_path="examples/go-processor/build/processor.wasm",
        config_path="examples/go-processor/config.yaml",
    )
    # Start function
    client.start_function("go-processor-example")
```

---

## 4. Lifecycle Management Matrix

The table below summarizes all standardized operations for daily maintenance of Functions:

| Operation | SQL Command                 | Python Client Interface    | Description                                  |
|-----------|-----------------------------|----------------------------|----------------------------------------------|
| Register  | CREATE FUNCTION WITH        | create_function_from_files | Persist logic and configuration to Server.   |
| List      | SHOW FUNCTIONS              | show_functions()           | View all tasks and their running status.     |
| Start     | START FUNCTION &lt;name&gt; | start_function(name)       | Allocate executors and start consuming data. |
| Stop      | STOP FUNCTION &lt;name&gt;  | stop_function(name)        | Pause task execution.                        |
| Drop      | DROP FUNCTION &lt;name&gt;  | drop_function(name)        | Physically delete task metadata and code.    |
