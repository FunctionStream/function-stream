#!/bin/bash
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WIT_DIR="$SCRIPT_DIR/wit"
BINDINGS_DIR="$SCRIPT_DIR/bindings"
BUILD_DIR="$SCRIPT_DIR/build"
OUTPUT_FILE="$BUILD_DIR/processor.wasm"

if ! command -v tinygo &> /dev/null; then
    echo "Error: tinygo not found"
    exit 1
fi

if ! command -v wit-bindgen-go &> /dev/null; then
    echo "Installing wit-bindgen-go..."
    go install github.com/bytecodealliance/wasm-tools-go/cmd/wit-bindgen-go@latest
fi

rm -rf "$BINDINGS_DIR" "$BUILD_DIR"

echo "Generating bindings..."
mkdir -p "$BINDINGS_DIR"
wit-bindgen-go generate --world processor --out "$BINDINGS_DIR" --cm "go.bytecodealliance.org/cm" "$WIT_DIR"

echo "Tidying dependencies..."
go mod tidy

echo "Building WASI P2 component..."
mkdir -p "$BUILD_DIR"
tinygo build -tags purego -scheduler=none -target=wasip2 -wit-package "$WIT_DIR" -wit-world processor -o "$OUTPUT_FILE" "$SCRIPT_DIR/main.go"

if command -v wasm-tools &> /dev/null; then
    echo "Validating component..."
    wasm-tools validate "$OUTPUT_FILE"
fi

echo "Build complete: $OUTPUT_FILE"