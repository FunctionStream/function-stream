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

SHELL := /usr/bin/env bash
.SHELLFLAGS := -eu -o pipefail -c

GO_SDK_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROJECT_ROOT := $(abspath $(GO_SDK_DIR)/..)
ROOT_WIT_DIR := $(PROJECT_ROOT)/wit
SDK_WIT_DIR := $(GO_SDK_DIR)/wit
BINDINGS_DIR := $(GO_SDK_DIR)/bindings

.PHONY: env wit bindings build clean

env:
	@command -v go >/dev/null 2>&1 || { echo "go is required"; exit 1; }
	@if ! command -v wit-bindgen-go >/dev/null 2>&1; then \
		go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest; \
	fi
	@if ! command -v wkg >/dev/null 2>&1; then \
		command -v cargo >/dev/null 2>&1 || { echo "cargo is required to install wkg"; exit 1; }; \
		cargo install wkg --version 0.10.0 --locked --quiet; \
	fi

wit: env
	@[ -f "$(ROOT_WIT_DIR)/processor.wit" ] || { echo "missing $(ROOT_WIT_DIR)/processor.wit"; exit 1; }
	@rm -rf "$(SDK_WIT_DIR)"
	@mkdir -p "$(SDK_WIT_DIR)"
	@cp "$(ROOT_WIT_DIR)/processor.wit" "$(SDK_WIT_DIR)/processor.wit"
	@if [ -d "$(ROOT_WIT_DIR)/deps" ] && [ -n "$$(ls -A "$(ROOT_WIT_DIR)/deps" 2>/dev/null)" ]; then \
		mkdir -p "$(SDK_WIT_DIR)/deps"; \
		cp -a "$(ROOT_WIT_DIR)/deps/." "$(SDK_WIT_DIR)/deps/"; \
		if [ -f "$(ROOT_WIT_DIR)/wkg.lock" ]; then cp "$(ROOT_WIT_DIR)/wkg.lock" "$(SDK_WIT_DIR)/wkg.lock"; fi; \
	else \
		wkg wit fetch --wit-dir "$(SDK_WIT_DIR)" -t wit >/dev/null; \
	fi

bindings: wit
	@rm -rf "$(BINDINGS_DIR)"
	@mkdir -p "$(BINDINGS_DIR)"
	@BIN="$$(command -v wit-bindgen-go || true)"; \
	if [ -z "$$BIN" ]; then BIN="$$(go env GOPATH)/bin/wit-bindgen-go"; fi; \
	"$$BIN" generate --world processor --out "$(BINDINGS_DIR)" --cm "go.bytecodealliance.org/cm" "$(SDK_WIT_DIR)"

build: bindings
	@cd "$(GO_SDK_DIR)" && go mod tidy && go test ./...

clean:
	@rm -rf "$(SDK_WIT_DIR)" "$(BINDINGS_DIR)"
