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

# -----------------------------------------------------------------------
# Integration Test Makefile
# -----------------------------------------------------------------------
# Usage:
#   make test        — Setup env + run pytest  (PYTEST_ARGS="-k xxx")
#   make clean       — Remove .venv and test output
#
# Prerequisites:
#   The FunctionStream binary must already be built (make build / make build-lite
#   from the project root).
# -----------------------------------------------------------------------

PROJECT_ROOT := $(shell git -C $(CURDIR) rev-parse --show-toplevel)
PYTHON_ROOT  := $(PROJECT_ROOT)/python
VENV         := $(CURDIR)/.venv
PIP          := $(VENV)/bin/pip
PY           := $(VENV)/bin/python

C_G := \033[0;32m
C_B := \033[0;34m
C_0 := \033[0m

log     = @printf "$(C_B)[-]$(C_0) %-12s %s\n" "$(1)" "$(2)"
success = @printf "$(C_G)[✔]$(C_0) %s\n" "$(1)"

.PHONY: test clean help

help:
	@echo "Integration Test Targets:"
	@echo ""
	@echo "  test    Setup Python env + run pytest (PYTEST_ARGS=...)"
	@echo "  clean   Remove .venv and target/tests output"

install: requirements.txt $(PYTHON_ROOT)/functionstream-api/pyproject.toml $(PYTHON_ROOT)/functionstream-client/pyproject.toml
	$(call log,ENV,Setting up Python virtual environment)
	@test -d $(VENV) || python3 -m venv $(VENV)
	@$(PIP) install --quiet --upgrade pip
	@$(PIP) install --quiet -r requirements.txt
	@touch $@
	$(call success,Python environment ready)

test: install
	$(call log,TEST,Running integration tests)
	@$(PY) -m pytest -v $(PYTEST_ARGS)
	$(call success,All integration tests passed)

clean:
	$(call log,CLEAN,Removing test artifacts)
	@rm -rf $(VENV)
	@rm -rf $(CURDIR)/target
	@rm -rf $(CURDIR)/install
	$(call success,Clean complete)
