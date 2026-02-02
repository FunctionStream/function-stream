#!/bin/bash

set -e

echo "Generating Go bindings..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WIT_FILE="$PROJECT_ROOT/wit/processor.wit"
OUT_DIR="$SCRIPT_DIR/bindings"

if [ ! -f "$WIT_FILE" ]; then
    echo "Error: WIT file not found: $WIT_FILE"
    exit 1
fi

# Check wit-bindgen-go
WIT_BINDGEN_GO=""
if command -v wit-bindgen-go &> /dev/null; then
    WIT_BINDGEN_GO="wit-bindgen-go"
elif [ -f "$HOME/go/bin/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$HOME/go/bin/wit-bindgen-go"
elif [ -n "$GOPATH" ] && [ -f "$GOPATH/bin/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$GOPATH/bin/wit-bindgen-go"
elif [ -n "$GOBIN" ] && [ -f "$GOBIN/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$GOBIN/wit-bindgen-go"
fi

if [ -z "$WIT_BINDGEN_GO" ]; then
    echo "Installing wit-bindgen-go..."
    go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest
    if [ -f "$HOME/go/bin/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$HOME/go/bin/wit-bindgen-go"
    elif [ -n "$GOPATH" ] && [ -f "$GOPATH/bin/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$GOPATH/bin/wit-bindgen-go"
    elif [ -n "$GOBIN" ] && [ -f "$GOBIN/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$GOBIN/wit-bindgen-go"
    else
        echo "Error: Unable to find wit-bindgen-go"
        exit 1
    fi
fi

# Create output directory
mkdir -p "$OUT_DIR"

# Generate bindings
echo "Running wit-bindgen-go generate..."
echo "WIT file: $WIT_FILE"
echo "Output directory: $OUT_DIR"
echo "World: processor"

"$WIT_BINDGEN_GO" generate "$WIT_FILE" --world="processor" --out="$OUT_DIR"

if [ $? -eq 0 ]; then
    echo "✓ Bindings generated to $OUT_DIR/ directory"
    echo ""
    echo "Notes:"
    echo "  - These binding files are intermediate artifacts used for compiling Go code"
    echo "  - Only one WASM file will be generated: build/processor.wasm"
    echo "  - The number of binding files depends on the number of interfaces defined in the WIT file (kv, collector, processor, etc.)"
    echo ""
    echo "Note: If you encounter Go 1.24 wasmimport/wasmexport compatibility issues,"
    echo "      please check if wit-bindgen-go has an updated version with support, or consider using other tools."
else
    echo "✗ Failed to generate bindings"
    exit 1
fi

