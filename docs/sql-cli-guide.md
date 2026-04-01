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

# SQL CLI Guide

Function Stream provides a SQL-like declarative interactive terminal (REPL), designed to provide operations personnel with low-threshold, high-efficiency task control capabilities.

---

## 1. Quick Access

SQL CLI communicates with the remote Server via the gRPC protocol.

### 1.1 Start Connection

**Release Package Method**: The CLI is provided with the release package. Before use, execute `make dist` to generate the release package, unzip it, and run `bin/start-cli.sh` in the `function-stream-<version>/` directory. The script relies on the `bin/cli` binary in the same directory and reads the Server port from `conf/config.yaml` (or specified via parameters).

```bash
./bin/start-cli.sh [options]
```

**start-cli.sh Options:**

| Option                | Description                                                                                                                |
|-----------------------|----------------------------------------------------------------------------------------------------------------------------|
| -h, --host \<HOST\>   | Server address, default 127.0.0.1. Can be overridden by environment variable FUNCTION_STREAM_HOST.                         |
| -p, --port \<PORT\>   | Server port, default parsed from conf/config.yaml or 8080. Can be overridden by environment variable FUNCTION_STREAM_PORT. |
| -c, --config \<PATH\> | Specify configuration file path for parsing Server port. Default conf/config.yaml.                                         |

**Example**: `./bin/start-cli.sh -h 10.0.0.1 -p 8080`

**Development Method**:

```bash
cargo run -p function-stream-cli -- -h <SERVER_HOST> -p <SERVER_PORT>
```

- **Terminal Prompt**: Displays `function-stream>` after successful connection.
- **Input Specification**: Supports multi-line input; the system detects statement completion and submits for execution via `;` (semicolon) or parenthesis balancing.

---

## 2. Command Syntax Details

### 2.1 Function Registration: CREATE FUNCTION

Used to associate pre-compiled WASM components with their topology configuration and register them to the server.

**Syntax Structure:**

```sql
CREATE FUNCTION WITH (
  'key1' = 'value1',
  'key2' = 'value2'
);
```

**Key Property Description:**

| Property Name | Required | Description                                                             |
|---------------|----------|-------------------------------------------------------------------------|
| function_path | Yes      | Absolute path of the WASM component on the Server node.                 |
| config_path   | Yes      | Absolute path of the task configuration file (YAML) on the Server node. |

**Operation Example:**

```sql
CREATE FUNCTION WITH (
  'function_path' = '/data/apps/processors/etl_v1.wasm',
  'config_path'   = '/data/apps/configs/etl_v1.yaml'
);
```

**Note**: The Function Name is automatically read from the YAML file pointed to by config_path and does not need to be explicitly specified in SQL.

### 2.2 Status Observation: SHOW FUNCTIONS

Retrieve metadata and running snapshots of all functions currently hosted by the Server.

```sql
SHOW FUNCTIONS;
```

**Output Field Parsing:**

- **name**: Unique function identifier name.
- **task_type**: Operator type (e.g., processor represents WASM).
- **status**: Current running status (Running, Stopped, Failed, Created).

### 2.3 Lifecycle Control: START / STOP / DROP

This set of commands directly controls the allocation and reclamation of computing resources.

**Start Task**: Load the function into the runtime engine and begin consuming upstream data.

```sql
START FUNCTION go_processor_demo;
```

**Stop Task**: Pause task execution.

```sql
STOP FUNCTION go_processor_demo;
```

**Physical Uninstall**: Completely erase the function information from metadata storage.

```sql
DROP FUNCTION go_processor_demo;
```

---

## 3. Streaming SQL: TABLE & STREAMING TABLE

In addition to Function management, the CLI supports a full set of **Streaming SQL** commands for declaring data sources and building real-time pipelines. For a comprehensive guide with examples, see [Streaming SQL Guide](streaming-sql-guide.md).

### 3.1 Register Data Source: CREATE TABLE

Declare an external data source (e.g. Kafka) with schema, event time, and watermark strategy. This creates a **static catalog entry** that consumes no compute resources.

```sql
CREATE TABLE ad_impressions (
    impression_id VARCHAR,
    ad_id BIGINT,
    campaign_id BIGINT,
    user_id VARCHAR,
    impression_time TIMESTAMP NOT NULL,
    WATERMARK FOR impression_time AS impression_time - INTERVAL '2' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_ad_impressions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
```

### 3.2 Create Streaming Pipeline: CREATE STREAMING TABLE

Launch a continuous, distributed compute pipeline using CTAS syntax. Results are written to the target connector in append-only mode.

```sql
CREATE STREAMING TABLE metric_tumble_impressions_1m WITH (
    'connector' = 'kafka',
    'topic' = 'sink_impressions_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    TUMBLE(INTERVAL '1' MINUTE) AS time_window,
    campaign_id,
    COUNT(*) AS total_impressions
FROM ad_impressions
GROUP BY 1, campaign_id;
```

### 3.3 Inspect & Monitor

| Command | Description |
|---------|-------------|
| `SHOW TABLES` | List all registered source tables. |
| `SHOW CREATE TABLE <name>` | Display the DDL of a registered table. |
| `SHOW STREAMING TABLES` | List all running streaming pipelines with status. |
| `SHOW CREATE STREAMING TABLE <name>` | Inspect the physical execution graph (ASCII topology). |

### 3.4 Destroy Streaming Pipeline: DROP STREAMING TABLE

Stop and release all resources for a streaming pipeline:

```sql
DROP STREAMING TABLE metric_tumble_impressions_1m;
```

---

## 4. REPL Built-in Auxiliary Commands

At the `function-stream>` prompt, the following convenient commands are supported:

| Command | Short | Description                                                               |
|---------|-------|---------------------------------------------------------------------------|
| help    | h     | Print all syntax templates supported by the current version.              |
| clear   | cls   | Clear screen output cache.                                                |
| exit    | q     | Safely disconnect gRPC connection and exit terminal. Supports quit or :q. |

---

## 5. Technical Constraints and Notes

- **Path Isolation**: The SQL CLI itself is not responsible for uploading files. The file pointed to by function_path must pre-exist on the **Server machine's** disk. If remote upload packaging is required, please use the Python SDK.
- **Python Function Limitations**: Since Python functions involve dynamic dependency analysis and code packaging, they are currently **not supported** for creation via SQL CLI; only lifecycle management such as START / STOP / SHOW via CLI is supported.
