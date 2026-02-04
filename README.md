# Function Stream

**Function Stream** is a high-performance, event-driven stream processing framework built in Rust. It provides a modular runtime to orchestrate serverless-style processing functions compiled to **WebAssembly (WASM)**, supporting functions written in **Go, Python, and Rust**.

## Key Features

* **Event-Driven WASM Runtime**: Executes polyglot functions (Go, Python, Rust) with near-native performance and sandboxed isolation.
* **Durable State Management**: Built-in support for RocksDB-backed state stores for stateful stream processing.
* **SQL-Powered CLI**: Interactive REPL for job management and stream inspection using SQL-like commands.

## Repository Layout

```text
function-stream/
├── src/                     # Core runtime, coordinator, server, config
├── protocol/                # Protocol Buffers definitions
├── cli/                     # SQL REPL client
├── conf/                    # Default runtime configuration
├── examples/                # Sample processors
├── python/                  # Python API, Client, and Runtime (WASM)
├── scripts/                 # Build and environment automation scripts
├── Makefile                 # Unified build system
└── Cargo.toml               # Workspace manifest
```

## Prerequisites

* **Rust Toolchain**: Stable >= 1.77 (via rustup).
* **Python 3.9+**: Required for building the Python WASM runtime.
* **Protoc**: Protocol Buffers compiler (for generating gRPC bindings).
* **Build Tools**: cmake, pkg-config, OpenSSL headers (for rdkafka).

## Quick Start (Local Development)

### 1. Initialize Environment

We provide an automated script to set up the Python virtual environment (`.venv`), install dependencies, and compile necessary sub-modules.

```bash
make env
```

This runs `scripts/setup.sh`, creating a `.venv` and installing the API, Client, and Runtime builder tools.

### 2. Build & Run Server

Start the control plane in release mode. The build system will automatically compile the Python WASM runtime if it's missing.

```bash
cargo run --release --bin function-stream
```

Logs are output to `logs/app.log` (JSON format) and stdout. Default configuration is loaded from `conf/config.yaml`.

### 3. Run CLI

In a separate terminal, launch the SQL REPL:

```bash
cargo run -p function-stream-cli -- --host 127.0.0.1 --port 8080
```

## Building & Packaging

The project uses a standard Makefile to handle compilation and distribution. Artifacts are generated in the `dist/` directory.

### Build Targets

| Command        | Description                                      |
|----------------|--------------------------------------------------|
| `make`         | Alias for `make build`. Compiles Rust binaries and Python WASM runtime. |
| `make build`   | Builds the Full version (Rust + Python Support). |
| `make build-lite` | Builds the Lite version (Rust only, no Python dependencies). |

### Distribution (Packaging)

To create production-ready archives (`.tar.gz` and `.zip`), use the dist targets.

#### 1. Function Stream (Full)

Includes the Rust binary, Python WASM runtime, configuration, and default directory structure.

```bash
make dist
```

**Output:**
* `dist/function-stream-<version>.tar.gz`
* `dist/function-stream-<version>.zip`

#### 2. Function Stream Lite

A lightweight distribution without the Python WASM runtime or dependencies.

```bash
make dist-lite
```

**Output:**
* `dist/function-stream-<version>-lite.tar.gz`
* `dist/function-stream-<version>-lite.zip`

## Maintenance

| Command        | Description                                                      |
|----------------|------------------------------------------------------------------|
| `make clean`   | Deep clean. Removes `target/`, `dist/`, `.venv/`, and all temporary artifacts. |
| `make env-clean` | Removes only the Python virtual environment and Python artifacts. |
| `make test`    | Runs the Rust test suite.                                        |

## Configuration

The runtime behavior is controlled by `conf/config.yaml`. You can override the configuration location using environment variables:

```bash
export FUNCTION_STREAM_CONF=/path/to/custom/config.yaml
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
