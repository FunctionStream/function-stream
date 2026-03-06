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

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
GO_SDK_DIR="$PROJECT_ROOT/go-sdk"

PROJECT_WIT_FILE="$GO_SDK_DIR/wit/processor.wit"
WIT_DEPS_DIR="$GO_SDK_DIR/wit/deps"

BUILD_DIR="$SCRIPT_DIR/build"
BUILD_WIT_DIR="$BUILD_DIR/wit"
OUTPUT_FILE="$BUILD_DIR/processor.wasm"

cd "$SCRIPT_DIR"

if ! command -v tinygo &> /dev/null; then
    printf "Error: 'tinygo' command not found.\n" >&2
    printf "TinyGo is required to build the WebAssembly component.\n" >&2
    printf "Please install TinyGo and ensure it is on your PATH.\n" >&2
    printf "See https://tinygo.org/getting-started/install/ for installation instructions.\n" >&2
    exit 1
fi

if ! command -v make &> /dev/null; then
    printf "Error: 'make' command not found.\n" >&2
    exit 1
fi

# 2. Cleanup & Sync Build Workspace
rm -rf "$BUILD_DIR"
mkdir -p "$BUILD_WIT_DIR/deps"

if [ ! -f "$PROJECT_WIT_FILE" ] || [ ! -d "$WIT_DEPS_DIR" ] || [ -z "$(ls -A "$WIT_DEPS_DIR" 2>/dev/null)" ]; then
    make -C "$GO_SDK_DIR" bindings
fi

if [ ! -f "$PROJECT_WIT_FILE" ]; then
    printf "Error: processor.wit not found at %s\n" "$PROJECT_WIT_FILE" >&2
    exit 1
fi

if [ ! -d "$WIT_DEPS_DIR" ] || [ -z "$(ls -A "$WIT_DEPS_DIR" 2>/dev/null)" ]; then
    printf "Error: WIT dependencies missing in %s\n" "$WIT_DEPS_DIR" >&2
    exit 1
fi

# Sync WIT files to build staging to avoid polluting source during codegen
cp "$PROJECT_WIT_FILE" "$BUILD_WIT_DIR/processor.wit"
cp -a "$WIT_DEPS_DIR/." "$BUILD_WIT_DIR/deps/"

# 4. Build Component
export GOFLAGS="${GOFLAGS:-} -mod=mod"
go mod tidy

mkdir -p "$(dirname "$OUTPUT_FILE")"

tinygo build -target=wasip2 \
    -tags purego \
    -wit-package "$BUILD_WIT_DIR" \
    -wit-world processor-runtime \
    -opt 2  \
    -scheduler none \
    -o "$OUTPUT_FILE" "$SCRIPT_DIR/main.go"

# 5. Optional Post-Build Validation
if command -v wasm-tools &> /dev/null; then
    wasm-tools validate "$OUTPUT_FILE"
fi

printf "Build successful: %s\n" "$OUTPUT_FILE"