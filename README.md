# Function Stream

**Function Stream** is a high-performance, event-driven stream processing framework built in Rust. It provides a modular runtime to orchestrate serverless-style processing functions compiled to **WebAssembly (WASM)**, supporting functions written in **Go, Python, and Rust**.



## Key Features

* **Event-Driven WASM Runtime**: Executes polyglot functions (Go, Python, Rust) with near-native performance and sandboxed isolation.
* **Durable State Management**: Built-in support for RocksDB-backed state stores for stateful stream processing.
* **SQL-Powered CLI**: Interactive REPL for job management and stream inspection using SQL-like commands.

## Repository Layout

```
function-stream/
├── src/                     # Core runtime, coordinator, server, config, SQL parser
├── protocol/                # Protocol Buffers definitions and generated gRPC code
├── cli/cli/                 # SQL REPL client
├── conf/                    # Default runtime configuration
├── examples/                # Sample processors and integration examples
├── python/                  # Python API, client, and runtime WASM generator
├── Makefile                 # Build and packaging automation
└── Cargo.toml               # Workspace manifest
```

## Prerequisites

- Rust toolchain (recommend `rustup` with stable >= 1.77)
- `protoc` (Protocol Buffers compiler) for generating gRPC bindings
- Build tooling for `rdkafka`: `cmake`, `pkg-config`, and OpenSSL headers
- Optional (for Python processors and full package):
  - Python 3.9+ with `venv`
  - `componentize-py` (installed via `make install-deps` inside `python/functionstream-runtime`)

## Build From Source

Clone the repository and install dependencies:

```
git clone https://github.com/<your-org>/function-stream.git
cd function-stream
cargo fetch
```

Build targets:

- Debug build (fast iteration):
  ```
  cargo build
  ```
- Release build with Python support (default features):
  ```
  cargo build --release
  ```
- Release build without Python (lite):
  ```
  cargo build --release --no-default-features --features incremental-cache
  ```

To regenerate Protocol Buffers, rerun the build; `tonic-build` automatically recompiles when `protocol/proto/function_stream.proto` changes.

## Run Locally

### Prepare configuration

1. Review `conf/config.yaml` and adjust service host/port, logging, state storage, and Python runtime settings as needed.
2. Optionally point `FUNCTION_STREAM_CONF` to a custom configuration file or directory:
   ```
   export FUNCTION_STREAM_CONF=/path/to/config.yaml
   ```
3. Ensure `data/` and `logs/` directories are writable (they are created automatically on startup).

### Start the control plane

```
cargo run --release --bin function-stream
```

The server logs appear under `logs/app.log` (default JSON format). Stop the service with `Ctrl+C`.

### Use the SQL CLI

Run the interactive CLI from another terminal to issue SQL statements:

```
cargo run -p function-stream-cli -- --ip 127.0.0.1 --port 8080
```

The CLI connects to the gRPC endpoint exposed by the server.

### Try sample processors

- Python processor example:
  ```
  cd examples/python-processor
  python main.py
  ```
- Kafka integration tests and Go processor examples are under `examples/`.

## Packaging

Packaging is orchestrated by the top-level `Makefile`. Outputs are placed in `dist/`.

### Full distribution (includes Python runtime)

1. Create the virtual environment once:
   ```
   python3 -m venv .venv
   source .venv/bin/activate
   make -C python/functionstream-runtime install-deps build
   deactivate
   ```
2. Build and package:
   ```
   make package-full
   ```

Artifacts:
- `dist/function-stream-<version>/` directory containing binaries, config, logs/data skeletons, README, and Python WASM runtime.
- `dist/packages/function-stream-<version>.zip` and `.tar.gz`.

### Lite distribution (Rust-only)

```
make package-lite
```

Artifacts:
- `dist/function-stream-<version>-lite/` directory with binaries and configs.
- `dist/packages/function-stream-<version>-lite.zip` and `.tar.gz`.

### Package all variants

```
make package-all
```

The script cleans previous `dist/` contents, produces both full and lite packages, and lists generated archives.

## Testing

Run the Rust test suite:

```
cargo test
```

Python components expose their own `Makefile` targets (for example, `make -C python/functionstream-runtime test` if defined).

## Environment Variables

- `FUNCTION_STREAM_HOME`: Overrides the project root for resolving data/log directories.
- `FUNCTION_STREAM_CONF`: Points to a configuration file or directory containing `config.yaml`.

## License

Licensed under the Apache License, Version 2.0. See `LICENSE` for details.
