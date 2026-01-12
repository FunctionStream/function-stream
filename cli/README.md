# CLI - Multi-language Client Libraries

This directory contains client libraries for rust-function-stream in multiple programming languages.

## Structure

- `cli-rust/` - Rust implementation (gRPC client library)
- `cli-go/` - Go implementation (to be implemented)
- `cli-java/` - Java implementation (to be implemented)
- `cli-python/` - Python implementation (to be implemented)

## Usage

### Rust

Add to your `Cargo.toml`:

```toml
[dependencies]
cli-rust = { path = "./cli/cli-rust" }
```

Use in your code:

```rust
use cli_rust::{CliClient, execute_login, execute_logout};

let mut client = CliClient::connect("http://127.0.0.1:8080").await?;
execute_login(&mut client, "admin".to_string(), "secret".to_string()).await?;
```

### Go

(To be implemented)

### Java

(To be implemented)

### Python

(To be implemented)

### TODO
