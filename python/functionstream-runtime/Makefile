# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Build script for Function Stream Runtime WASM component

.PHONY: all build clean install-deps test

# 目录路径
SCRIPT_DIR := $(shell dirname $(realpath $(firstword $(MAKEFILE_LIST))))
PROJECT_ROOT := $(shell dirname $(SCRIPT_DIR))
WIT_DIR := $(PROJECT_ROOT)/wit
OUTPUT_DIR := $(SCRIPT_DIR)/target
WASM_OUTPUT := $(OUTPUT_DIR)/functionstream-runtime.wasm

# 默认目标
all: build

# 构建 WASM 组件
build:
	@echo "Building WASM component..."
	@mkdir -p $(OUTPUT_DIR)
	@python3 $(SCRIPT_DIR)/build.py

# 清理构建产物
clean:
	@echo "Cleaning build artifacts..."
	@rm -rf $(OUTPUT_DIR)
	@rm -rf $(SCRIPT_DIR)/__pycache__
	@rm -rf $(SCRIPT_DIR)/fs_runtime/__pycache__
	@rm -rf $(SCRIPT_DIR)/dependencies
	@rm -rf $(SCRIPT_DIR)/*.egg-info
	@echo "✓ Clean completed"

# 安装依赖
install-deps:
	@echo "Installing dependencies..."
	@python3 -m pip install -e ../functionstream-api
	@python3 -m pip install componentize-py
	@echo "✓ Dependencies installed"

# 安装所有相关包
install-all:
	@echo "Installing all packages..."
	@python3 -m pip install -e ../functionstream-api
	@python3 -m pip install -e ../functionstream-client
	@python3 -m pip install -e .
	@python3 -m pip install componentize-py
	@echo "✓ All packages installed"

# 帮助信息
help:
	@echo "Function Stream Runtime - WASM Build"
	@echo ""
	@echo "Available targets:"
	@echo "  make build        - Build WASM component"
	@echo "  make clean        - Clean build artifacts"
	@echo "  make install-deps - Install dependencies"
	@echo "  make install-all  - Install all packages"
	@echo "  make help         - Show this help message"

