# Go Function Example

This example demonstrates how to write a WebAssembly-based Function Stream Processor in Go, implementing a real-time counter (same logic as the Python example).

## Features

- **Input**: Kafka `input-topic`
- **Processing**: Word frequency counting with KV state
- **Output**: JSON format to Kafka `output-topic`

## Prerequisites

- **TinyGo**: <https://tinygo.org/getting-started/install/>
- **wasm-tools**: `cargo install wasm-tools`
- **Go SDK toolchain**: run `make -C go-sdk env` from project root

## Build

```bash
make -C ../../go-sdk build
./build.sh
# Output: build/processor.wasm
```

## SQL Operations

### Create Function

```sql
create function with ('function_path'='/path/to/examples/go-processor/build/processor.wasm','config_path'='/path/to/examples/go-processor/config.yaml')
```

Example:
```sql
create function with ('function_path'='/Users/zhenyuluo/lyy/data/git/function-stream/examples/go-processor/build/processor.wasm','config_path'='/Users/zhenyuluo/lyy/data/git/function-stream/examples/go-processor/config.yaml')
```

### Show Functions

```sql
show functions
```

### Stop Function

```sql
stop function <function-name>
```

Example:
```sql
stop function go-processor-example
```

### Drop Function

```sql
drop function <function-name>
```

Example:
```sql
drop function go-processor-example
```

## Complete Example Workflow

```bash
# 1. Build the WASM processor
./build.sh

# 2. Start the CLI
cd ../../
./scripts/start-cli.sh

# 3. In the CLI, create and manage the function
```

```sql
-- Create function
create function with ('function_path'='/Users/zhenyuluo/lyy/data/git/function-stream/examples/go-processor/build/processor.wasm','config_path'='/Users/zhenyuluo/lyy/data/git/function-stream/examples/go-processor/config.yaml')

-- Show all functions
show functions

-- Stop function (if running)
stop function go-processor-example

-- Drop function
drop function go-processor-example
```

## Example Input/Output

**Input** (from Kafka `input-topic`):
```
hello
world
hello
hello
world
```

**Output** (to Kafka `output-topic`):
```json
{"total_processed":1,"counter_map":{"hello":1}}
{"total_processed":2,"counter_map":{"hello":1,"world":1}}
{"total_processed":3,"counter_map":{"hello":2,"world":1}}
{"total_processed":4,"counter_map":{"hello":3,"world":1}}
{"total_processed":5,"counter_map":{"hello":3,"world":2}}
```

## Implementation Details

- **Language**: Go (compiled with TinyGo)
- **Target**: WASI P2 (WebAssembly System Interface Preview 2)
- **State Management**: KV store with key prefix configuration
- **Lifecycle**: 
  - `fs-init`: Initialize state and configuration
  - `fs-process`: Process each message
  - `fs-close`: Cleanup resources

## Configuration

The function requires a configuration file (`config.yaml`) that defines:

### config.yaml Structure

```yaml
name: "go-processor-example"
type: processor
input-groups:
  - inputs:
      - input-type: kafka
        bootstrap_servers: "localhost:9092"
        topic: "input-topic"
        partition: 0
        group_id: "go-processor-group"
outputs:
  - output-type: kafka
    bootstrap_servers: "localhost:9092"
    topic: "output-topic"
    partition: 0
```

### Configuration Parameters

| Parameter | Type | Description | Example |
|-----------|------|-------------|---------|
| `name` | string | Unique function name | `"go-processor-example"` |
| `type` | string | Function type | `processor` |
| `input-groups` | array | Input source configurations | See above |
| `outputs` | array | Output sink configurations | See above |

### Runtime Configuration

The processor also accepts runtime configuration via `fs-init`:

| Parameter | Type | Description | Default |
|-----------|------|-------------|---------|
| `key_prefix` | string | Prefix for KV store keys | `""` |

## Architecture

```
┌─────────────┐
│ Kafka Source│
│ input-topic │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│  Go WASM Processor  │
│  ┌───────────────┐  │
│  │  fs-process   │  │
│  │  ┌─────────┐  │  │
│  │  │ Counter │  │  │
│  │  │  Logic  │  │  │
│  │  └────┬────┘  │  │
│  │       │       │  │
│  │  ┌────▼────┐  │  │
│  │  │ KV Store│  │  │
│  │  └─────────┘  │  │
│  └───────────────┘  │
└──────────┬──────────┘
           │
           ▼
    ┌─────────────┐
    │ Kafka Sink  │
    │output-topic │
    └─────────────┘
```

## Troubleshooting

### Build Fails with Missing WIT Files

If you see errors about missing WIT files or WASI interfaces:

```bash
make -C ../../go-sdk bindings
./build.sh
```

### Component Validation Fails

Ensure you have the latest version of wasm-tools:

```bash
cargo install wasm-tools --force
```

### TinyGo Version Issues

This example requires TinyGo 0.40.0 or later with WASI P2 support:

```bash
tinygo version
# Should show: tinygo version 0.40.0 or higher
```

## Files

```
examples/go-processor/
├── build.sh              # Build script
├── main.go               # Go processor implementation
├── go.mod                # Go module definition
├── config.yaml           # Function configuration (Kafka I/O)
└── build/                # Build output
    └── processor.wasm    # WASI P2 component

go-sdk/
├── Makefile              # Go SDK build pipeline
├── runtime.go            # SDK runtime bootstrap
├── context.go            # SDK context APIs
├── store.go              # SDK state store APIs
├── wit/                  # Generated WIT package and dependencies
└── bindings/             # Generated Go bindings
```

## See Also

- [Function Management Guide](../../docs/function-management-zh.md)
- [SQL CLI Guide](../../docs/sql-cli-zh.md)
- [Python Processor Example](../python-processor/main.py)
