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
PYTHON_ROOT="$ROOT_DIR/python"

C_R="\033[0;31m"
C_G="\033[0;32m"
C_B="\033[0;34m"
C_Y="\033[0;33m"
C_0="\033[0m"

log_act() { printf "${C_B} [-]${C_0} %s\n" "$1"; }
log_ok()  { printf "${C_G} [✔]${C_0} %s\n" "$1"; }

purge_artifacts() {
    local target="$1"
    if [ ! -d "$target" ]; then return; fi
    rm -rf "$target/build" "$target/dist" "$target/"*.egg-info "$target/.eggs"
    rm -rf "$target/.pytest_cache" "$target/.mypy_cache"
    find "$target" -type d \( -name "__pycache__" -o -name "*.egg-info" \) -print0 2>/dev/null | xargs -0 rm -rf 2>/dev/null || true
    find "$target" -type f \( -name "*.pyc" -o -name "*.pyo" -o -name "*.pyd" \) -delete 2>/dev/null || true
}

cd "$ROOT_DIR"

if [ -d "$VENV_DIR" ]; then
    log_act ".venv"
    rm -rf "$VENV_DIR"
fi

MODULES=("functionstream-api" "functionstream-client" "functionstream-runtime")
for mod in "${MODULES[@]}"; do
    MOD_PATH="$PYTHON_ROOT/$mod"
    if [ -d "$MOD_PATH" ]; then
        log_act "$mod"
        purge_artifacts "$MOD_PATH"
    fi
done

RT_DIR="$PYTHON_ROOT/functionstream-runtime"
if [ -d "$RT_DIR" ]; then
    rm -rf "$RT_DIR/target" "$RT_DIR/bindings" "$RT_DIR/dependencies" 2>/dev/null || true
fi

PROTO_DIR="$PYTHON_ROOT/functionstream-client/src/fs_client/_proto"
if [ -d "$PROTO_DIR" ]; then
    find "$PROTO_DIR" -maxdepth 1 -type f \( \
        -name "*_pb2.py" -o \
        -name "*_pb2.pyi" -o \
        -name "*_pb2_grpc.py" -o \
        -name "*_pb2_grpc.pyi" \
    \) -delete 2>/dev/null || true
fi

EXAMPLES_DIR="$ROOT_DIR/examples/python-processor"
if [ -d "$EXAMPLES_DIR" ]; then
    purge_artifacts "$EXAMPLES_DIR"
fi

log_ok "Done"
