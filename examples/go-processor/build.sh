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
#!/bin/bash

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

WIT_DIR="$SCRIPT_DIR/wit"
PROJECT_WIT_FILE="$PROJECT_ROOT/wit/processor.wit"
WIT_DEPS_DIR="$PROJECT_ROOT/wit/deps"

BINDINGS_DIR="$SCRIPT_DIR/bindings"
BUILD_DIR="$SCRIPT_DIR/build"
BUILD_WIT_DIR="$BUILD_DIR/wit"
OUTPUT_FILE="$BUILD_DIR/processor.wasm"

cd "$SCRIPT_DIR"

# 1. Validation & Environment Check
if [ ! -f "$PROJECT_WIT_FILE" ]; then
    printf "Error: processor.wit not found at %s\n" "$PROJECT_WIT_FILE" >&2
    exit 1
fi

if [ ! -d "$WIT_DEPS_DIR" ] || [ -z "$(ls -A "$WIT_DEPS_DIR" 2>/dev/null)" ]; then
    printf "Error: WIT dependencies missing in %s\n" "$WIT_DEPS_DIR" >&2
    printf "Please run 'make env' first to resolve dependencies.\n" >&2
    exit 1
fi

if ! command -v tinygo &> /dev/null; then
    exit 1
fi

if ! command -v wit-bindgen-go &> /dev/null; then
    go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest
fi

# 2. Cleanup & Sync Build Workspace
rm -rf "$BINDINGS_DIR" "$BUILD_DIR"
mkdir -p "$BINDINGS_DIR" "$BUILD_WIT_DIR/deps"

# Sync WIT files to build staging to avoid polluting source during codegen
cp "$PROJECT_WIT_FILE" "$BUILD_WIT_DIR/processor.wit"
cp -a "$WIT_DEPS_DIR/." "$BUILD_WIT_DIR/deps/"

if ! command -v wit-bindgen-go &> /dev/null; then
    echo "wit-bindgen-go not found. Installing..."
    go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest
    export PATH=$PATH:$(go env GOPATH)/bin
fi

wit-bindgen-go generate \
    --world processor \
    --out "$BINDINGS_DIR" \
    --cm "go.bytecodealliance.org/cm" \
    "$BUILD_WIT_DIR"

# 4. Build Component
export GOFLAGS="${GOFLAGS:-} -mod=mod"
go mod tidy

mkdir -p "$(dirname "$OUTPUT_FILE")"

tinygo build -target=wasip2 \
    -tags purego \
    -wit-package "$BUILD_WIT_DIR" \
    -wit-world processor-runtime \
    -o "$OUTPUT_FILE" "$SCRIPT_DIR/main.go"

# 5. Optional Post-Build Validation
if command -v wasm-tools &> /dev/null; then
    wasm-tools validate "$OUTPUT_FILE"
fi

printf "Build successful: %s\n" "$OUTPUT_FILE"