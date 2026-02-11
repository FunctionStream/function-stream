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

# Server Configuration

Function Stream is a cloud-native stream processing platform that supports lightweight computational tasks written in WASM (Go/Rust) and Python.

---

## 1. Server Configuration

The Server is controlled by `conf/config.yaml`, which manages networking, logging, the execution engine, and persistent storage.

### 1.1 Basic Service

| Parameter  | Type   | Required | Default         | Description & Recommendation                                                                       |
|------------|--------|----------|-----------------|----------------------------------------------------------------------------------------------------|
| service_id | string | No       | default-service | Unique instance identifier. Recommended to set to hostname in cluster deployments.                 |
| host       | string | Yes      | 127.0.0.1       | Listening address. Set to 0.0.0.0 if you need to provide service externally or run in a container. |
| port       | int    | Yes      | 8080            | gRPC service port.                                                                                 |
| workers    | int    | No       | CPU Cores × 4   | Thread pool depth. Recommended to increase appropriately for I/O-intensive tasks.                  |
| debug      | bool   | No       | false           | If enabled, Trace level logs will be recorded. Recommended to disable in production environments.  |

### 1.2 Logging System

| Parameter     | Type   | Default      | Description & Recommendation                                           |
|---------------|--------|--------------|------------------------------------------------------------------------|
| level         | string | info         | Log level: trace, debug, info, warn, error.                            |
| format        | string | json         | Recommended to set to json for structured collection with ELK or Loki. |
| file_path     | string | logs/app.log | Log storage path.                                                      |
| max_file_size | int    | 100          | Maximum size of a single file (MB).                                    |
| max_files     | int    | 5            | Number of historical log files to retain (rolling).                    |

---

## 2. Runtime Configuration

Python configuration focuses on the interpreter environment, while WASM configuration focuses on component compilation optimization.

### 2.1 Python Runtime (python)

| Parameter    | Type   | Default       | Description & Recommendation                                                                    |
|--------------|--------|---------------|-------------------------------------------------------------------------------------------------|
| wasm_path    | string | (Built-in)    | Critical. Points to the Python runtime WASM engine. Startup will fail if the path is incorrect. |
| cache_dir    | string | data/cache/py | Storage location for AOT (Ahead-of-Time) compilation artifacts. Ensure read/write permissions.  |
| enable_cache | bool   | true          | Core setting. Enabling this eliminates the second-level delay of Python task cold starts.       |

### 2.2 WASM Runtime (wasm)

| Parameter      | Type   | Default     | Description & Recommendation                                                       |
|----------------|--------|-------------|------------------------------------------------------------------------------------|
| cache_dir      | string | .cache/wasm | Stores intermediate machine code after incremental compilation of WASM components. |
| enable_cache   | bool   | true        | Enables incremental compilation cache to accelerate function logic hot updates.    |
| max_cache_size | int    | 100MB       | Cache limit. Recommended to increase to 512MB if deploying more than 50 functions. |

---

## 3. Storage Configuration

The system uses RocksDB for persistence of task metadata and state.

| Configuration Item (RocksDB) | Default | Description & Recommendation                                                                   |
|------------------------------|---------|------------------------------------------------------------------------------------------------|
| storage_type                 | rocksdb | Must use rocksdb in production environments; memory mode is for temporary testing only.        |
| base_dir                     | data    | Data root directory. Recommended to mount on SSD storage volumes to ensure I/O throughput.     |
| max_open_files               | 1000    | Number of file handles RocksDB is allowed to open. Needs to be adjusted with system ulimit -n. |
| write_buffer_size            | 64MB    | Write buffer size. Recommended to increase to 128MB for very high write frequency.             |
| target_file_size_base        | 64MB    | Target size for L0 SST files.                                                                  |

---

## 4. Task Development and Management

### 4.1 Task YAML Configuration Specification

Each Function needs to define a set of I/O topologies:

- **name / type**: Task name and type (processor / python).
- **input-groups**: Defines input sources (e.g., Kafka bootstrap_servers, topic, group_id).
- **outputs**: Defines output channels (e.g., Kafka topic, partition).

See [Function Task Configuration Specification](function-configuration.md) for details.

### 4.2 Management Commands (SQL CLI)

Manage the full lifecycle of functions via SQL REPL:

- **Create**: `CREATE FUNCTION WITH ('function_path'='...', 'config_path'='...');`
- **Start/Stop**: `START FUNCTION <name>;` / `STOP FUNCTION <name>;`
- **Show/Drop**: `SHOW FUNCTIONS;` / `DROP FUNCTION <name>;`

---

## 5. Operations

### 5.1 Startup Script (bin/start-server.sh)

This script is provided with the release package. Before use, execute `make dist` to generate the release package, unzip it, and run it in the `function-stream-<version>/` directory. The script relies on the `bin/function-stream` binary and `conf/config.yaml` in the same directory.

- **Run in background**: `./bin/start-server.sh -d`
- **Custom configuration**: `./bin/start-server.sh -c /path/to/config.yaml`
