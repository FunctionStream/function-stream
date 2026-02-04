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

inject_env_var() {
  local string="$1"
  local key=$(echo "$string" | cut -d'=' -f1)
  local value=$(echo "$string" | cut -d'=' -f2-)

  if [ -n "$key" ]; then
    export "$key"="$value"
  fi
}

check_port_usage() {
  local config_file="$1"
  local port=""

  if [ -f "$config_file" ]; then
    port=$(grep -E "^[[:space:]]*port:" "$config_file" | head -n 1 | awk '{print $2}' | tr -d '"' | tr -d "'")
  fi

  port=${port:-8080}

  if ! command -v lsof >/dev/null 2>&1 && ! command -v netstat >/dev/null 2>&1; then
    return 0
  fi

  echo "Checking port usage: $port..."
  local pid=""

  if command -v lsof >/dev/null 2>&1; then
    pid=$(lsof -t -i:"$port" -sTCP:LISTEN 2>/dev/null || true)
  elif command -v netstat >/dev/null 2>&1; then
    pid=$(netstat -anp 2>/dev/null | grep ":$port " | grep 'LISTEN' | awk '{print $NF}' | sed "s|/.*||g" || true)
  fi

  if [ -n "$pid" ]; then
    echo "[ERROR] Port $port is already occupied by PID $pid"
    return 1
  fi
  return 0
}

BIN_DIR=$(get_real_path)
APP_HOME="$( cd -P "$BIN_DIR/.." && pwd )"

export FUNCTION_STREAM_HOME="$APP_HOME"
export FUNCTION_STREAM_CONF="${FUNCTION_STREAM_CONF:-$APP_HOME/conf/config.yaml}"
export FUNCTION_STREAM_LOG_DIR="${FUNCTION_STREAM_LOG_DIR:-$APP_HOME/logs}"

BINARY="$BIN_DIR/function-stream"
FOREGROUND="true"
APP_ARGS=""

if [ ! -f "$BINARY" ]; then
  echo "[ERROR] Binary not found at $BINARY"
  exit 1
fi

while [ $# -gt 0 ]; do
  case "$1" in
    -c|--config)
      export FUNCTION_STREAM_CONF="$2"
      shift 2
      ;;
    -d|--daemon)
      FOREGROUND="false"
      shift
      ;;
    -D)
      inject_env_var "$2"
      shift 2
      ;;
    --)
      shift
      APP_ARGS="$APP_ARGS $*"
      break
      ;;
    *)
      APP_ARGS="$APP_ARGS $1"
      shift
      ;;
  esac
done

mkdir -p "$FUNCTION_STREAM_LOG_DIR"

check_port_usage "$FUNCTION_STREAM_CONF" || exit 1

echo "------------------------------------------------"
echo "Starting Function Stream"
echo "Home:   $FUNCTION_STREAM_HOME"
echo "Config: $FUNCTION_STREAM_CONF"
echo "Mode:   $( [ "$FOREGROUND" = "true" ] && echo "Foreground" || echo "Daemon" )"
echo "------------------------------------------------"

if [ "$FOREGROUND" = "true" ]; then
  exec "$BINARY" --config "$FUNCTION_STREAM_CONF" $APP_ARGS
else
  LOG_OUT="$FUNCTION_STREAM_LOG_DIR/stdout.log"
  LOG_ERR="$FUNCTION_STREAM_LOG_DIR/stderr.log"

  nohup "$BINARY" --config "$FUNCTION_STREAM_CONF" $APP_ARGS > "$LOG_OUT" 2> "$LOG_ERR" &
  PID=$!

  sleep 1
  if kill -0 "$PID" >/dev/null 2>&1; then
    echo "[SUCCESS] Started. PID: $PID"
    echo "Logs: $LOG_OUT"
  else
    echo "[FAILED] Process exited immediately."
    echo "Check error log: $LOG_ERR"
    exit 1
  fi
fi
