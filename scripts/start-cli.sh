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
#

set -eo pipefail

get_real_path() {
  local source="${BASH_SOURCE[0]}"
  while [ -h "$source" ]; do
    local dir="$( cd -P "$( dirname "$source" )" && pwd )"
    source="$(readlink "$source")"
    [[ $source != /* ]] && source="$dir/$source"
  done
  echo "$( cd -P "$( dirname "$source" )" && pwd )"
}

BIN_DIR=$(get_real_path)
APP_HOME="$( cd -P "$BIN_DIR/.." && pwd )"

export FUNCTION_STREAM_HOME="$APP_HOME"
export FUNCTION_STREAM_CONF="${FUNCTION_STREAM_CONF:-$APP_HOME/conf/config.yaml}"

BINARY="$BIN_DIR/cli"
CLI_HOST="${FUNCTION_STREAM_HOST:-127.0.0.1}"
CLI_PORT="${FUNCTION_STREAM_PORT:-}"

if [ ! -f "$BINARY" ]; then
  echo "[ERROR] CLI binary not found at $BINARY"
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    -h|--host)
      CLI_HOST="$2"
      shift 2
      ;;
    -p|--port)
      CLI_PORT="$2"
      shift 2
      ;;
    -c|--config)
      export FUNCTION_STREAM_CONF="$2"
      shift 2
      ;;
    *)
      shift
      ;;
  esac
done

if [ -z "$CLI_PORT" ] && [ -f "$FUNCTION_STREAM_CONF" ]; then
  CONFIG_PORT=$(grep -E "^[[:space:]]*port:" "$FUNCTION_STREAM_CONF" | head -n 1 | awk '{print $2}' | tr -d '"' | tr -d "'")
  CLI_PORT="${CONFIG_PORT:-8080}"
fi
CLI_PORT="${CLI_PORT:-8080}"

echo "------------------------------------------------"
echo "Function Stream SQL CLI"
echo "Home:   $FUNCTION_STREAM_HOME"
echo "Server: $CLI_HOST:$CLI_PORT"
echo "------------------------------------------------"

exec "$BINARY" -h "$CLI_HOST" -p "$CLI_PORT"
