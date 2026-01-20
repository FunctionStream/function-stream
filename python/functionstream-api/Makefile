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
PACKAGE_NAME := functionstream_api

# Shell setup (Fail fast on errors)
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

# Paths
mkfile_path := $(abspath $(lastword $(MAKEFILE_LIST)))
current_dir := $(dir $(mkfile_path))
PROJECT_ROOT := $(current_dir)
DIST_DIR     := $(PROJECT_ROOT)/dist
BUILD_DIR    := $(PROJECT_ROOT)/build

# Virtual Environment Configuration
VENV_DIR     := $(PROJECT_ROOT)../../.venv
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

define log_error
    @echo "$(color_red)[ERROR]$(color_reset) $1"
endef

define log_warn
    @echo "$(color_yellow)[WARN]$(color_reset) $1"
endef

# --- 3. Targets ---

.PHONY: all help venv install install-dev build clean test publish info uninstall

# Default target
all: build

# Create virtual environment if it doesn't exist
$(PYTHON):
	$(call log_info, Creating virtual environment in .venv...)
	@python3 -m venv $(VENV_DIR)
	@$(PIP) install --upgrade pip setuptools wheel build twine pytest

venv: $(PYTHON)

# Install package in standard mode (Release simulation)
install: venv
	$(call log_info, Installing $(PROJECT_NAME)...)
	@$(PIP) install .
	$(call log_success, Installation complete.)
	@echo "Verify with: $(PYTHON) -c 'import $(PACKAGE_NAME); print($(PACKAGE_NAME).__file__)'"

# Install in editable mode (Development)
install-dev: venv
	$(call log_info, Installing $(PROJECT_NAME) in editable mode...)
	@# Try installing with [dev] extras, fall back to standard editable
	@$(PIP) install -e ".[dev]" || $(PIP) install -e .
	$(call log_success, Editable installation complete.)

# Build Source and Wheel distribution
build: venv clean
	$(call log_info, Building distribution artifacts...)
	@$(PYTHON) -m build
	$(call log_info, Checking distribution artifacts with Twine...)
	@$(TWINE) check dist/*
	$(call log_success, Build complete. Artifacts in 'dist/'.)

# Publish to PyPI (Requires credentials)
publish: build
	$(call log_warn, You are about to upload to PyPI.)
	@$(TWINE) upload dist/*

# Run tests
test: venv
	$(call log_info, Running tests...)
	@if [ -d "tests" ]; then \
    	$(PYTEST) tests/ -v; \
	else \
		$(call log_warn, No 'tests' directory found. Skipping.); \
	fi

# Deep clean (Fixed the truncation here)
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

# Uninstall from venv
uninstall:
	$(call log_info, Uninstalling $(PROJECT_NAME)...)
	@$(PIP) uninstall -y $(PROJECT_NAME) || true
	$(call log_success, Uninstallation complete.)

# Show environment info
info: venv
	@echo "$(color_blue)Project Info:$(color_reset)"
	@echo "  Name:    $(PROJECT_NAME)"
	@echo "  Package: $(PACKAGE_NAME)"
	@echo "  Root:    $(PROJECT_ROOT)"
	@echo "  Python:  $(shell $(PYTHON) --version)"
	@echo "  Venv:    $(VENV_DIR)"
	@echo ""
	@echo "$(color_blue)Installed Dependencies:$(color_reset)"
	@$(PIP) list

# Auto-generated help
help:
	@echo "$(color_blue)$(PROJECT_NAME) Build System$(color_reset)"
	@echo ""
	@echo "Usage:"
	@echo "  make $(color_green)<target>$(color_reset)"
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