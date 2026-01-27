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

# Project Metadata
PROJECT_NAME := functionstream-client
PACKAGE_NAME := fs_client

# Shell setup (Fail fast)
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Directory paths (Absolute paths for safety)
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(dir $(mkfile_path))

# [Standard Monorepo Roots]
SCRIPT_DIR    := $(current_dir)
PYTHON_ROOT   := $(abspath $(current_dir)/..)
MONOREPO_ROOT := $(abspath $(current_dir)/../..)

# Output & Source Paths
SRC_DIR       := $(SCRIPT_DIR)/src
DIST_DIR      := $(SCRIPT_DIR)/dist
BUILD_DIR     := $(SCRIPT_DIR)/build
PROTO_OUT_DIR := $(SRC_DIR)/$(PACKAGE_NAME)/_proto

# Virtual Environment (Shared)
VENV_DIR      := $(MONOREPO_ROOT)/.venv
VENV_BIN      := $(VENV_DIR)/bin
PYTHON        := $(VENV_BIN)/python
PIP           := $(VENV_BIN)/pip
TWINE         := $(VENV_BIN)/twine
BLACK         := $(VENV_BIN)/black
ISORT         := $(VENV_BIN)/isort
MYPY          := $(VENV_BIN)/mypy

# --- 2. Colors & Logging ---
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

define log_warn
    @echo "$(shell tput setaf 3)[WARN]$(color_reset) $1"
endef

# --- 3. Targets ---

.PHONY: all venv install-deps install install-dev proto build clean lint format publish info help

# Default target
all: install-dev proto build

# Check venv
$(PYTHON):
	$(call log_warn, Virtual environment not found at $(VENV_DIR))
	$(call log_info, Please run 'python3 -m venv .venv' in the Monorepo root first.)
	@exit 1

venv: $(PYTHON)

# Install Dependencies (Runtime + Dev + Build tools)
install-deps: venv
	$(call log_info, Installing dependencies...)
	@$(PIP) install --upgrade pip setuptools wheel build twine
	@$(PIP) install grpcio-tools mypy-protobuf mypy black isort types-protobuf
	@$(call log_success, Dependencies installed.)

# Install package in editable mode (Development)
install-dev: install-deps
	$(call log_info, Installing $(PROJECT_NAME) in editable mode...)
	@$(PIP) install -e .
	$(call log_success, Editable installation complete.)

# Generate Protobuf/gRPC code
proto: venv
	$(call log_info, Generating gRPC code from protocols...)
	@# Ensure output directory exists and is a package
	@mkdir -p $(PROTO_OUT_DIR)
	@touch $(PROTO_OUT_DIR)/__init__.py
	@# Delegate to the python script, but ensure it runs in the venv
	@$(PYTHON) scripts/codegen.py
	$(call log_success, Proto code generated.)

# Code Formatting (Auto-fix)
format: venv
	$(call log_info, Formatting code with Black and isort...)
	@$(ISORT) $(SRC_DIR)
	@$(BLACK) $(SRC_DIR)
	$(call log_success, Formatting complete.)

# Code Linting (Checks only)
lint: venv
	$(call log_info, Linting code...)
	@$(ISORT) --check-only $(SRC_DIR)
	@$(BLACK) --check $(SRC_DIR)
	@$(MYPY) $(SRC_DIR) --ignore-missing-imports || true
	$(call log_success, Lint checks passed.)

# Build Distribution (Wheel & Sdist)
build: venv clean proto
	$(call log_info, Building distribution artifacts...)
	@$(PYTHON) -m build
	$(call log_info, Checking artifacts with Twine...)
	@$(TWINE) check dist/*
	$(call log_success, Build complete. Artifacts in 'dist/'.)

# Deep Clean
clean:
	$(call log_info, Cleaning build artifacts...)
	@rm -rf $(DIST_DIR) $(BUILD_DIR)
	@rm -rf *.egg-info .eggs
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type d -name ".mypy_cache" -exec rm -rf {} +
	@# Clean generated proto files (be careful not to delete __init__.py if manually maintained)
	@rm -f $(PROTO_OUT_DIR)/*_pb2.py
	@rm -f $(PROTO_OUT_DIR)/*_pb2.pyi
	@rm -f $(PROTO_OUT_DIR)/*_pb2_grpc.py
	@rm -f $(PROTO_OUT_DIR)/*_pb2_grpc.pyi
	$(call log_success, Clean completed.)

# Publish to PyPI
publish: build
	$(call log_warn, You are about to upload to PyPI.)
	@$(TWINE) upload dist/*

# Project Info
info: venv
	@echo "$(color_blue)Project Info:$(color_reset)"
	@echo "  Project: $(PROJECT_NAME)"
	@echo "  Package: $(PACKAGE_NAME)"
	@echo "  Venv:    $(VENV_DIR)"
	@echo "  Python:  $(shell $(PYTHON) --version)"

# Help
help:
	@echo "$(color_blue)$(PROJECT_NAME) Build System$(color_reset)"
	@echo "Usage: make <target>"
	@echo ""
	@echo "Targets:"
	@awk '/^[a-zA-Z\-\_0-9]+:/ { \
	   helpMessage = match(lastLine, /^# (.*)/); \
	   if (helpMessage) { \
		  helpCommand = substr($$1, 0, index($$1, ":")-1); \
		  helpMessage = substr(lastLine, RSTART + 2, RLENGTH); \
		  printf "  $(color_green)%-15s$(color_reset) %s\n", helpCommand, helpMessage; \
	   } \
	} \
	{ lastLine = $$0 }' $(MAKEFILE_LIST)