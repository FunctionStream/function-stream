# Integration Tests

## Prerequisites

| Dependency | Version  | Purpose                                         |
|------------|----------|-------------------------------------------------|
| Python     | >= 3.9   | Test framework runtime                          |
| Rust       | stable   | Build the FunctionStream binary                 |
| Docker     | >= 20.10 | Run a Kafka broker for streaming integration tests |

> **Docker is required.** The test framework automatically pulls and manages
> an `apache/kafka:3.7.0` container in KRaft mode to provide a real Kafka
> broker for tests that involve Kafka input/output. Tests will fail if the
> Docker daemon is not running.

## Quick Start

```bash
# From the project root
make build          # Build the release binary (with --features python)
make integration-test

# Or run directly from this directory
cd tests/integration
make test
```

## Directory Layout

```
tests/integration/
├── Makefile              # test / clean targets
├── requirements.txt      # Python dependencies (pytest, grpcio, docker, ...)
├── pytest.ini            # Pytest configuration
├── framework/            # Reusable test infrastructure
│   ├── instance.py       # FunctionStreamInstance facade
│   ├── workspace.py      # Per-test directory management
│   ├── config.py         # Server config generation
│   ├── process.py        # OS process lifecycle (start/stop/kill)
│   ├── utils.py          # Port allocation, readiness probes
│   └── kafka_manager.py  # Docker-managed Kafka broker (KRaft mode)
├── test/                 # Test suites
│   ├── wasm/             # WASM function tests
│   │   └── python_sdk/   # Python SDK integration tests
│   └── streaming/        # Streaming engine tests (future)
└── target/               # Test output (git-ignored)
    ├── .shared_cache/    # Shared WASM compilation cache across tests
    └── <suite>/<class>/<method>/<timestamp>/logs/
```

## Test Output

Each test gets an isolated server instance with its own log directory:

```
target/wasm/python_sdk/TestFunctionLifecycle/test_full_lifecycle_transitions/20260416_221655/
  logs/
    app.log       # FunctionStream application log
    stdout.log    # Server stdout
    stderr.log    # Server stderr
```

Only `logs/` is retained after tests complete; `conf/` and `data/` are
automatically cleaned up.

## Python Dependencies

All Python packages are listed in `requirements.txt` and installed
automatically by `make test`. Key dependencies:

- `pytest` — test runner
- `grpcio` / `protobuf` — gRPC client communication
- `docker` — Docker SDK for managing the Kafka container
- `confluent-kafka` — Kafka admin client for topic management
- `functionstream-api` / `functionstream-client` — local editable installs

## Running Specific Tests

```bash
# Single test
make test PYTEST_ARGS="-k test_full_lifecycle_transitions"

# Single file
make test PYTEST_ARGS="test/wasm/python_sdk/test_lifecycle.py"

# Verbose with live log
make test PYTEST_ARGS="-v --log-cli-level=DEBUG"
```
