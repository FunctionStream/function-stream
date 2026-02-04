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

### 2. Build & Run Server

Start the control plane in release mode. The build system will automatically compile the Python WASM runtime if it's missing.

```bash
cargo run --release --bin function-stream
```

### 3. Run CLI

In a separate terminal, launch the SQL REPL:

```bash
cargo run -p function-stream-cli -- --host 127.0.0.1 --port 8080
```

## Building & Packaging

The project uses a standard Makefile to handle compilation and distribution. Artifacts are generated in the `dist/` directory.

### Build Targets

| Command          | Description                                                       |
|------------------|------------------------------------------------------------------|
| `make`           | Alias for `make build`. Compiles Rust binaries and Python WASM runtime. |
| `make build`     | Builds the Full version (Rust + Python Support).                   |
| `make build-lite`| Builds the Lite version (Rust only, no Python dependencies).     |

### Distribution (Packaging)

To create production-ready archives (`.tar.gz` and `.zip`), use the dist targets.

```bash
make dist
```

**Output:**
* `dist/function-stream-<version>.tar.gz`
* `dist/function-stream-<version>.zip`

For a lightweight distribution without Python WASM runtime:

```bash
make dist-lite
```

**Output:**
* `dist/function-stream-<version>-lite.tar.gz`
* `dist/function-stream-<version>-lite.zip`

### Running the Distribution

After extracting the release package, you will see the following structure:

```text
function-stream-<version>/
├── bin/
│   ├── function-stream      # The compiled binary
│   └── start-server.sh      # Production startup script
├── conf/
│   └── config.yaml          # Default configuration
├── data/                    # Runtime data (e.g. WASM cache)
└── logs/                    # Log directory (created on runtime)
```

### Using the Startup Script (bin/start-server.sh)

We provide a robust shell script to manage the server process, capable of handling environment injection, daemonization, and configuration overrides.

**Usage:**

```bash
./bin/start-server.sh [options]
```

**Options:**

| Option              | Description                                                                 |
|---------------------|-----------------------------------------------------------------------------|
| `-d`, `--daemon`    | Run the server in the background (Daemon mode). Writes PID to `function-stream.pid`. |
| `-c`, `--config <path>` | Specify a custom configuration file path.                              |
| `-p`, `--pidfile <path>` | Specify a custom location for the PID file.                           |
| `-D <KEY>=<VALUE>`  | Inject environment variables (e.g., `-D RUST_LOG=debug`).                   |

**Examples:**

1. **Foreground Mode (Docker / Debugging)** — Runs the server in the current shell. Useful for containerized environments (Kubernetes/Docker) or debugging.

   ```bash
   ./bin/start-server.sh
   ```

2. **Daemon Mode (Production)** — Runs the server in the background, redirects stdout/stderr to `logs/`, and manages the PID file.

   ```bash
   ./bin/start-server.sh -d
   ```

3. **Custom Configuration** — Start with a specific config file and write the PID to a specific location.

   ```bash
   ./bin/start-server.sh -d -c /etc/function-stream/prod.yaml -p /var/run/fs.pid
   ```

4. **Environment Variable Injection** — Inject environment variables without modifying the script or system env.

   ```bash
   ./bin/start-server.sh -d -D RUST_LOG=debug
   ```

## Maintenance

| Command          | Description                                                                 |
|------------------|-----------------------------------------------------------------------------|
| `make clean`     | Deep clean. Removes `target/`, `dist/`, `.venv/`, and all temporary artifacts. |
| `make env-clean` | Removes only the Python virtual environment and Python artifacts.           |
| `make test`      | Runs the Rust test suite.                                                    |

## Configuration

The runtime behavior is controlled by `conf/config.yaml`. You can override the configuration location using environment variables:

```bash
export FUNCTION_STREAM_CONF=/path/to/custom/config.yaml
```

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) for details.
