#!/usr/bin/env bash
#
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
ROOT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$ROOT_DIR/.venv"
VENV_BIN="$VENV_DIR/bin"
PIP="$VENV_BIN/pip"
PYTHON_BIN="$VENV_BIN/python"
PYTHON_ROOT="$ROOT_DIR/python"

C_G="\033[0;32m"
C_Y="\033[0;33m"
C_B="\033[0;34m"
C_0="\033[0m"

log_step() { printf "${C_B}>> [%s]${C_0}\n" "$1"; }
log_ok()   { printf "${C_G} [OK]${C_0}\n"; }
log_warn() { printf "${C_Y} [!] %s${C_0}\n" "$1"; }

cd "$ROOT_DIR"

log_step "VENV"
if [ ! -d "$VENV_DIR" ]; then
    python3 -m venv "$VENV_DIR"
    log_ok
else
    log_warn "$VENV_DIR"
fi

log_step "CORE_TOOLS"
"$PIP" install -q --upgrade pip setuptools wheel build twine
log_ok

log_step "WIT_DEPS"
WIT_SRC_DIR="$ROOT_DIR/wit"
PROCESSOR_WIT="$WIT_SRC_DIR/processor.wit"
WIT_DEPS_DIR="$WIT_SRC_DIR/deps"

download_wasi_wit_deps() {
    [[ -f "$PROCESSOR_WIT" ]] || exit 1

    if ! command -v wkg &> /dev/null; then
        command -v cargo &> /dev/null || exit 1
        cargo install wkg --version 0.10.0 --locked --quiet
    fi

    # 1. Define staging_dir as local
    local staging_dir
    staging_dir=$(mktemp -d)

    # 2. Industrial Fix: Use a safer trap.
    # '${staging_dir:-}' prevents 'unbound variable' error during script EXIT.
    # We use both RETURN (for function end) and EXIT (for script crash).
    trap '[[ -n "${staging_dir:-}" ]] && rm -rf "$staging_dir"' RETURN EXIT

    mkdir -p "$staging_dir/wit"
    cp "$PROCESSOR_WIT" "$staging_dir/wit/processor.wit"

    local build_log="$staging_dir/error.log"

    if ! (cd "$staging_dir" && wkg wit fetch > "$build_log" 2>&1); then
        printf "${C_Y} [!] wkg fetch failed:${C_0}\n" >&2
        cat "$build_log" >&2
        exit 1
    fi

    rm -rf "$WIT_DEPS_DIR"
    mkdir -p "$WIT_DEPS_DIR"

    if [ -d "$staging_dir/wit/deps" ]; then
        cp -a "$staging_dir/wit/deps/." "$WIT_DEPS_DIR/"
    fi

    if [ -f "$staging_dir/wkg.lock" ]; then
        cp "$staging_dir/wkg.lock" "$WIT_SRC_DIR/wkg.lock"
    fi

    # 3. Explicitly clear trap for this scope to prevent double-run at EXIT
    trap - RETURN EXIT
    rm -rf "$staging_dir"
}

download_wasi_wit_deps
log_ok

log_step "API_INSTALL"
"$PIP" install -q -e "$PYTHON_ROOT/functionstream-api"
"$PIP" install -q -e "$PYTHON_ROOT/functionstream-api-advanced"
log_ok

log_step "CLIENT_CODEGEN"
"$PIP" install -q grpcio-tools mypy-protobuf
mkdir -p "$PYTHON_ROOT/functionstream-client/src/fs_client/_proto"
touch "$PYTHON_ROOT/functionstream-client/src/fs_client/_proto/__init__.py"
(cd "$PYTHON_ROOT/functionstream-client" && "$PYTHON_BIN" scripts/codegen.py)
log_ok

log_step "CLIENT_INSTALL"
"$PIP" install -q -e "$PYTHON_ROOT/functionstream-client"
log_ok

log_step "RUNTIME_DEPS"
"$PIP" install -q componentize-py
log_ok

log_step "API_BUILD"
(cd "$PYTHON_ROOT/functionstream-api" && "$PYTHON_BIN" -m build > /dev/null)
"$VENV_BIN/twine" check "$PYTHON_ROOT/functionstream-api/dist"/* > /dev/null 2>&1 || true
log_ok

log_step "CLIENT_BUILD"
(cd "$PYTHON_ROOT/functionstream-client" && "$PYTHON_BIN" -m build > /dev/null)
"$VENV_BIN/twine" check "$PYTHON_ROOT/functionstream-client/dist"/* > /dev/null 2>&1 || true
log_ok

log_step "WASM_BUILD"
TARGET_DIR="$ROOT_DIR/data/cache/python-runner"
mkdir -p "$TARGET_DIR"
(cd "$PYTHON_ROOT/functionstream-runtime" && PYTHONPATH="$PYTHON_ROOT/functionstream-api:$PYTHON_ROOT/functionstream-api-advanced" "$PYTHON_BIN" build.py > /dev/null)

WASM_SRC="$PYTHON_ROOT/functionstream-runtime/target/functionstream-python-runtime.wasm"
if [ -f "$WASM_SRC" ]; then
    cp "$WASM_SRC" "$TARGET_DIR/"
    log_ok
fi

log_step "GO_SDK_BUILD"
command -v make > /dev/null 2>&1 || exit 1
if [ -d "$ROOT_DIR/go-sdk" ]; then
    make -C "$ROOT_DIR/go-sdk" build > /dev/null
fi
log_ok
