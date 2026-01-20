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

# --- 1. Configuration & Variables ---

# Shell setup
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Directory paths (Absolute paths for safety)
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(dir $(mkfile_path))

# [FIX 1] Define standard roots for Monorepo
# SCRIPT_DIR: Current directory (.../python/functionstream-runtime)
SCRIPT_DIR   := $(current_dir)

# PYTHON_ROOT: Parent directory (.../python) - Used to find siblings like 'api'
PYTHON_ROOT  := $(abspath $(current_dir)/..)

# MONOREPO_ROOT: Grandparent directory (.../function-stream) - Used to find .venv
MONOREPO_ROOT := $(abspath $(current_dir)/../..)

# Output paths
OUTPUT_DIR   := $(SCRIPT_DIR)/target
WASM_OUTPUT  := $(OUTPUT_DIR)/functionstream-runtime.wasm

# [FIX 2] Virtual Environment (Located at Monorepo Root)
VENV_DIR     := $(MONOREPO_ROOT)/.venv
VENV_BIN     := $(VENV_DIR)/bin
PYTHON       := $(VENV_BIN)/python
PIP          := $(VENV_BIN)/pip
COMPONENTIZE := $(VENV_BIN)/componentize-py

# --- 2. Colors & logging ---
ifneq ($(shell test -t 0; echo $$?),0)
    color_reset :=
    color_green :=
    color_blue  :=
    color_red   :=
else
    color_reset := $(shell tput sgr0)
    color_green := $(shell tput setaf 2)
    color_blue  := $(shell tput setaf 4)
    color_red   := $(shell tput setaf 1)
endif

define log_info
    @echo "$(color_blue)[INFO]$(color_reset) $1"
endef

define log_success
    @echo "$(color_green)[SUCCESS]$(color_reset) $1"
endef

define log_error
    @echo "$(color_red)[ERROR]$(color_reset) $1"
endef

# --- 3. Targets ---

.PHONY: all build clean install-deps venv check-env help

# Default target
all: build

# Ensure venv exists (Delegate to root venv creation if needed, or just warn)
$(PYTHON):
	$(call log_info, Virtual environment not found at $(VENV_DIR))
	$(call log_info, Please run 'python3 -m venv .venv' in the root 'function-stream' directory first.)
	@exit 1

venv: $(PYTHON)

# Install dependencies
install-deps: venv
	$(call log_info, Installing build dependencies...)
	@$(PIP) install componentize-py
	@# [FIX 3] Use PYTHON_ROOT to find the sibling 'functionstream-api'
	@$(PIP) install -e $(PYTHON_ROOT)/functionstream-api
	@$(call log_success, Dependencies installed.)

# Build the WASM component
build: venv
	@# Check if componentize-py is installed
	@if [ ! -f "$(COMPONENTIZE)" ]; then \
		echo "$(color_red)[ERROR]$(color_reset) componentize-py not found. Run 'make install-deps' first."; \
		exit 1; \
	fi
	$(call log_info, Building WASM component...)
	@mkdir -p $(OUTPUT_DIR)
	@# [FIX 4] PYTHONPATH points to the sibling API correctly
	@PYTHONPATH=$(PYTHON_ROOT)/functionstream-api $(PYTHON) $(SCRIPT_DIR)/build.py
	@if [ -f "$(WASM_OUTPUT)" ]; then \
		echo "$(color_green)[SUCCESS]$(color_reset) Build complete: $(WASM_OUTPUT)"; \
		ls -lh $(WASM_OUTPUT); \
	else \
		echo "$(color_red)[ERROR]$(color_reset) Build failed: Output file missing"; \
		exit 1; \
	fi

# Clean artifacts
clean:
	$(call log_info, Cleaning artifacts...)
	@rm -rf $(OUTPUT_DIR)
	@rm -rf $(SCRIPT_DIR)/bindings
	@rm -rf $(SCRIPT_DIR)/dependencies
	@find $(SCRIPT_DIR) -type d -name "__pycache__" -exec rm -rf {} +
	@find $(SCRIPT_DIR) -type d -name "*.egg-info" -exec rm -rf {} +
	@find $(SCRIPT_DIR) -type d -name ".pytest_cache" -exec rm -rf {} +
	$(call log_success, Clean completed.)

# Self-documenting help command
help:
	@echo "$(color_blue)Function Stream Runtime Build System$(color_reset)"
	@echo ""
	@echo "Usage:"
	@echo "  make $(color_green)target$(color_reset)"
	@echo ""
	@echo "Targets:"
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
	   helpMessage = match(lastLine, /^# (.*)/); \
	   if (helpMessage) { \
		  helpCommand = substr($$1, 0, index($$1, ":")-1); \
		  helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
		  printf "  $(color_green)%-20s$(color_reset) %s\n", helpCommand, helpMessage; \
	   } \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)