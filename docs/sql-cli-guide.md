# SQL CLI Interactive Management Guide

Function Stream provides a SQL-like declarative interactive terminal (REPL), designed to provide operations personnel with low-threshold, high-efficiency cluster task control capabilities.

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
| -i, --ip \<IP\>       | Server address, default 127.0.0.1. Can be overridden by environment variable FUNCTION_STREAM_IP.                           |
| -P, --port \<PORT\>   | Server port, default parsed from conf/config.yaml or 8080. Can be overridden by environment variable FUNCTION_STREAM_PORT. |
| -c, --config \<PATH\> | Specify configuration file path for parsing Server port. Default conf/config.yaml.                                         |

**Example**: `./bin/start-cli.sh -i 10.0.0.1 -P 8080`

**Development Method**:

```bash
cargo run -p function-stream-cli -- -i <SERVER_IP> -P <SERVER_PORT>
```

- **Terminal Prompt**: Displays `sql>` after successful connection.
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

## 3. REPL Built-in Auxiliary Commands

At the `sql>` prompt, in addition to standard SQL statements, the following convenient commands are also supported:

| Command | Short | Description                                                               |
|---------|-------|---------------------------------------------------------------------------|
| help    | h     | Print all syntax templates supported by the current version.              |
| clear   | cls   | Clear screen output cache.                                                |
| exit    | q     | Safely disconnect gRPC connection and exit terminal. Supports quit or :q. |

---

## 4. Technical Constraints and Notes

- **Path Isolation**: The SQL CLI itself is not responsible for uploading files. The file pointed to by function_path must pre-exist on the **Server machine's** disk. If remote upload packaging is required, please use the Python SDK.
- **Python Function Limitations**: Since Python functions involve dynamic dependency analysis and code packaging, they are currently **not supported** for creation via SQL CLI; only lifecycle management such as START / STOP / SHOW via CLI is supported.
