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

# ==============================================================================
#  Function Stream API - Industrial Build System
# ==============================================================================

# --- 1. Configuration & Variables ---

# Project Metadata
PROJECT_NAME := functionstream-api
PACKAGE_NAME := fs_api

# Shell setup
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Directory paths
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(dir $(mkfile_path))

# [Standard Monorepo Roots]
# SCRIPT_DIR: Current directory (.../python/functionstream-api)
SCRIPT_DIR   := $(current_dir)
# MONOREPO_ROOT: Grandparent directory (.../function-stream)
MONOREPO_ROOT := $(abspath $(current_dir)/../..)

# Output paths
DIST_DIR     := $(SCRIPT_DIR)/dist
BUILD_DIR    := $(SCRIPT_DIR)/build

# Virtual Environment (Shared)
VENV_DIR     := $(MONOREPO_ROOT)/.venv
VENV_BIN     := $(VENV_DIR)/bin
PYTHON       := $(VENV_BIN)/python
PIP          := $(VENV_BIN)/pip
TWINE        := $(VENV_BIN)/twine
PYTEST       := $(VENV_BIN)/pytest

# --- 2. Colors & Logging ---
ifneq ($(shell test -t 0; echo $$?),0)
    color_reset :=
    color_green :=
    color_blue  :=
    color_red   :=
    color_yellow:=
else
    color_reset := $(shell tput sgr0)
    color_green := $(shell tput setaf 2)
    color_blue  := $(shell tput setaf 4)
    color_red   := $(shell tput setaf 1)
    color_yellow:= $(shell tput setaf 3)
endif

define log_info
    @echo "$(color_blue)[INFO]$(color_reset) $1"
endef

define log_success
    @echo "$(color_green)[SUCCESS]$(color_reset) $1"
endef

define log_warn
    @echo "$(color_yellow)[WARN]$(color_reset) $1"
endef

# --- 3. Targets ---

.PHONY: all help venv install-deps install install-dev build clean test publish info uninstall

# Default target
all: build

# Ensure venv exists (Check root)
$(PYTHON):
	$(call log_warn, Virtual environment not found at $(VENV_DIR))
	$(call log_info, Please run 'python3 -m venv .venv' in the Monorepo root first.)
	@exit 1

venv: $(PYTHON)

# [FIX] Explicitly install build tools (Idempotent)
install-deps: venv
	$(call log_info, Installing build dependencies...)
	@$(PIP) install --upgrade pip setuptools wheel build twine pytest
	@$(call log_success, Build dependencies installed.)

# Install package (Release simulation)
install: install-deps
	$(call log_info, Installing $(PROJECT_NAME)...)
	@$(PIP) install .
	$(call log_success, Installation complete.)

# Install in editable mode (Development)
install-dev: install-deps
	$(call log_info, Installing $(PROJECT_NAME) in editable mode...)
	@$(PIP) install -e ".[dev]" || $(PIP) install -e .
	$(call log_success, Editable installation complete.)

# Build Source and Wheel distribution
# [FIX] Added install-deps as a prerequisite
build: install-deps clean
	$(call log_info, Building distribution artifacts...)
	@$(PYTHON) -m build
	$(call log_info, Checking artifacts with Twine...)
	@$(TWINE) check dist/*
	$(call log_success, Build complete. Artifacts in 'dist/'.)

# Publish to PyPI
publish: build
	$(call log_warn, You are about to upload to PyPI.)
	@$(TWINE) upload dist/*

# Run tests
test: install-deps
	$(call log_info, Running tests...)
	@if [ -d "tests" ]; then \
		$(PYTEST) tests/ -v; \
	else \
		$(call log_warn, No 'tests' directory found. Skipping.); \
	fi

# Deep clean
clean:
	$(call log_info, Cleaning build artifacts...)
	@rm -rf $(DIST_DIR) $(BUILD_DIR)
	@rm -rf *.egg-info .eggs
	@find . -type d -name "__pycache__" -exec rm -rf {} +
	@find . -type d -name "*.egg-info" -exec rm -rf {} +
	@find . -type d -name ".pytest_cache" -exec rm -rf {} +
	@find . -type d -name ".coverage" -exec rm -rf {} +
	@find . -type f -name "*.pyc" -delete
	@find . -type f -name "*.pyo" -delete
	$(call log_success, Clean complete.)

# Uninstall
uninstall:
	$(call log_info, Uninstalling $(PROJECT_NAME)...)
	@$(PIP) uninstall -y $(PROJECT_NAME) || true
	$(call log_success, Uninstallation complete.)

# Info
info: venv
	@echo "$(color_blue)Project Info:$(color_reset)"
	@echo "  Name:    $(PROJECT_NAME)"
	@echo "  Package: $(PACKAGE_NAME)"
	@echo "  Venv:    $(VENV_DIR)"
	@echo "  Python:  $(shell $(PYTHON) --version)"
	@echo ""
	@echo "$(color_blue)Installed Packages:$(color_reset)"
	@$(PIP) list | grep -E "build|wheel|twine|$(PROJECT_NAME)"

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