<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# Python SDK Guide

Function Stream provides a complete toolchain for Python developers, covering the entire process from function development (fs_api) to management (fs_client).

---

## 1. SDK Core Component Definition

| Package Name | Positioning                     | Core Function                                                                                          |
|--------------|---------------------------------|--------------------------------------------------------------------------------------------------------|
| fs_api       | Operator Development Interface  | Defines Processor logic, provides KV state access and data emission (Emit) capabilities.               |
| fs_client    | Cluster Control Client          | Based on gRPC, implements remote registration, state control, and topology configuration of functions. |
| fs_runtime   | Built-in Runtime (Internal Use) | Encapsulates Python interpreter behavior within the WASM isolated environment.                         |

---

## 2. fs_api: Deep Dive into Operator Development

### 2.1 Core Base Class FSProcessorDriver

All Python operators must inherit from this class. It is the entry point for logic execution, and its lifecycle methods are automatically triggered by the server.

| Hook Method                | Trigger Timing                             | Business Logic Suggestion                                                              |
|----------------------------|--------------------------------------------|----------------------------------------------------------------------------------------|
| init(ctx, config)          | Executed once when function starts         | Custom init_config.                                                                    |
| process(ctx, source, data) | Triggered upon receiving each message      | Core logic location. Perform calculation, state read/write, and result distribution.   |
| process_watermark(...)     | Triggered upon receiving watermark events  | Handle time-based window triggers or out-of-order reordering logic.                    |
| take_checkpoint(ctx, id)   | Callback when system performs state backup | Return additional in-memory state that needs persistence to ensure strong consistency. |
| check_heartbeat(ctx)       | Periodic health check                      | Check internal operators; returning False will trigger operator restart.               |

### 2.2 Stateful Calculation: Context and KvStore

The power of Function Stream lies in its built-in local state management.

**Context Interaction:**

- `ctx.emit(bytes, channel=0)`: Push processing results to the specified output channel.
- `ctx.getOrCreateKVStore("name")`: Access RocksDB-based local state storage.

**KvStore Interface:**

- Supports basic `put_state` / `get_state`.
- Advanced support for ComplexKey operations, suitable for multi-dimensional indexing or prefix scanning scenarios.

### 2.3 Production-Grade Code Example

```python
from fs_api import FSProcessorDriver, Context
import json

class MetricProcessor(FSProcessorDriver):
    def init(self, ctx: Context, config: dict):
        self.metric_name = config.get("metric_name", "default_event")

    def process(self, ctx: Context, source_id: int, data: bytes):
        # 1. Parse input
        event = json.loads(data.decode())
        
        # 2. Atomic state operation: Accumulate count
        store = ctx.getOrCreateKVStore("stats_db")
        key = f"count:{self.metric_name}".encode()
        current_val = int(store.get_state(key) or 0)
        
        new_val = current_val + event.get("value", 1)
        store.put_state(key, str(new_val).encode())
        
        # 3. Emit processing result
        result = {"metric": self.metric_name, "total": new_val}
        ctx.emit(json.dumps(result).encode(), 0)
```

---

## 3. fs_client: Function Management

### 3.1 Function Creation Process: Automated Dependency Packaging

Unlike WASM mode which requires manual compilation, fs_client performs the following operations when registering Python functions:

1. **Static Analysis**: Automatically scans the module where ProcessorClass is located and its referenced local dependencies.
2. **Resource Packaging**: Encapsulates code and its dependencies into a specific format.
3. **Remote Registration**: Uploads via gRPC streaming to the Server and launches execution within the sandbox.

### 3.2 Chained Configuration Construction (WasmTaskBuilder)

Using the Builder pattern allows for clear definition of the function's I/O topology:

```python
from fs_client.config import WasmTaskBuilder, KafkaInput, KafkaOutput

task_config = (
    WasmTaskBuilder()
    .set_name("python-etl-job")
    .add_init_config("metric_name", "user_click")
    .add_input_group([
        KafkaInput(bootstrap_servers="kafka:9092", topic="raw-data", group_id="fs-group")
    ])
    .add_output(KafkaOutput(bootstrap_servers="kafka:9092", topic="clean-data", partition=0))
    .build()
)
```

### 3.3 Client Interaction Full Flow

```python
from fs_client import FsClient

with FsClient(host="10.0.0.1", port=8080) as client:
    # 1. Register Python operator
    client.create_python_function_from_config(task_config, MetricProcessor)
    
    # 2. Start function
    client.start_function("python-etl-job")
    
    # 3. Monitor status
    status = client.show_functions(filter_pattern="python-etl-job")
    print(f"Task Status: {status.functions[0].status}")
```

---

## 4. Operations and Exception Handling Matrix

| Exception Class       | Trigger Cause                                                                     | Recommended Handling Strategy                                 |
|-----------------------|-----------------------------------------------------------------------------------|---------------------------------------------------------------|
| ConflictError (409)   | Attempting to register an existing function name                                  | Call drop_function first or modify the task name.             |
| BadRequestError (400) | YAML configuration does not meet specifications or Kafka parameters are incorrect | Check configuration items in WasmTaskBuilder.                 |
| ServerError (500)     | Server-side runtime environment (e.g., RocksDB) exception                         | Check permissions of storage path in server conf/config.yaml. |
| NotFoundError (404)   | Operating on a non-existent function or invalid Checkpoint                        | Confirm if the function name is correct.                      |

---

## 5. Advanced State API

For typed state (ValueState, ListState, MapState, Keyed\* factories, etc.) and `from_context` / `from_context_auto_codec` usage, see the dedicated document:

- **[Python SDK — Advanced State API](python-sdk-advanced-state-api.md)**
