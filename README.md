# Rust Function Stream

## Project Structure

```
rust-function-stream/
├── src/
│   ├── client/
│   │   ├── mod.rs       # Client module entry
│   │   └── api.rs       # Client API implementation
│   ├── config/
│   │   ├── mod.rs       # Configuration module entry and exports
│   │   ├── types.rs     # Global configuration type definitions and registry
│   │   └── loader.rs    # YAML reading functions
│   ├── server/
│   │   ├── mod.rs       # Server module entry
│   │   ├── connection_manager.rs # Connection state manager
│   │   ├── session_manager.rs    # Session manager
│   │   └── service.rs            # gRPC service implementation
│   ├── lib.rs           # Library entry
│   └── main.rs          # Demo program
├── protocol/            # Pure protocol definition package
│   ├── proto/
│   │   └── function_stream.proto # Protocol Buffers definitions
│   ├── src/
│   │   ├── lib.rs       # Protocol exports
│   │   └── function_stream.rs # Generated message code
│   └── Cargo.toml       # Protocol package configuration
├── Cargo.toml           # Main project configuration
└── README.md            # Documentation
```
