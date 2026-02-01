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

VERSION := $(shell grep '^version' Cargo.toml | head -1 | sed 's/version = "\(.*\)"/\1/' | tr -d ' ')
ARCH := $(shell uname -m)
OS := $(shell uname -s | tr '[:upper:]' '[:lower:]')
BUILD_DIR := target/release
DIST_BASE := dist
FULL_DIR := $(DIST_BASE)/function-stream-$(VERSION)
LITE_DIR := $(DIST_BASE)/function-stream-$(VERSION)-lite
PACKAGE_DIR := packages
PYTHON_WASM_PATH := python/functionstream-runtime/target/functionstream-python-runtime.wasm
PYTHON_WASM_NAME := functionstream-python-runtime.wasm

.PHONY: help clean clean-dist build build-full build-lite package package-full package-lite package-all test install docker-build docker-up

help:
	@echo "Function Stream Build System"
	@echo ""
	@echo "Available targets:"
	@echo "  build          - Build full version (debug)"
	@echo "  build-full     - Build full release version"
	@echo "  build-lite     - Build lite release version (no Python)"
	@echo "  package-full   - Build and package full version (.zip and .tar.gz)"
	@echo "  package-lite   - Build and package lite version (.zip and .tar.gz)"
	@echo "  package-all    - Build and package both versions"
	@echo "  docker-build   - Run package-full then docker-compose build"
	@echo "  docker-up     - Run package-full then docker-compose up -d"
	@echo "  test          - Run tests"
	@echo "  clean          - Clean all build artifacts (cargo + dist)"
	@echo "  clean-dist     - Clean distribution directory only"
	@echo ""
	@echo "Version: $(VERSION)"
	@echo "Architecture: $(ARCH)"
	@echo "OS: $(OS)"

clean:
	@echo "Cleaning build artifacts..."
	cargo clean
	@rm -rf $(DIST_BASE)
	@echo "Clean complete"

clean-dist:
	@echo "Cleaning distribution directory..."
	@rm -rf $(DIST_BASE)
	@echo "Distribution directory cleaned"

build:
	cargo build

build-full:
	@echo "Building full release version..."
	@if [ ! -f $(PYTHON_WASM_PATH) ]; then \
		echo "Python WASM file not found. Building it first..."; \
		cd python/functionstream-runtime && $(MAKE) build || { \
			echo "Error: Failed to build Python WASM file"; \
			exit 1; \
		}; \
	fi
	cargo build --release --features python
	@echo "Full version build complete"

build-lite:
	@echo "Building lite release version (no Python)..."
	cargo build --release --no-default-features --features incremental-cache
	@echo "Lite version build complete"

prepare-dist-full:
	@echo "Preparing full distribution..."
	@if [ -d $(FULL_DIR) ]; then \
		echo "Removing existing full distribution directory..."; \
		rm -rf $(FULL_DIR); \
	fi
	@mkdir -p $(FULL_DIR)/bin
	@mkdir -p $(FULL_DIR)/conf
	@mkdir -p $(FULL_DIR)/data/cache/python-runner
	@mkdir -p $(FULL_DIR)/logs
	@if [ -f $(BUILD_DIR)/function-stream ]; then \
		cp $(BUILD_DIR)/function-stream $(FULL_DIR)/bin/ && \
		chmod +x $(FULL_DIR)/bin/function-stream && \
		echo "Copied function-stream binary"; \
	fi
	@if [ -f LICENSE ]; then \
		cp LICENSE $(FULL_DIR)/ && \
		echo "Copied LICENSE file"; \
	else \
		echo "Warning: LICENSE file not found"; \
	fi
	@if [ -f README.md ]; then \
		cp README.md $(FULL_DIR)/ && \
		echo "Copied README.md"; \
	fi
	@if [ -f $(PYTHON_WASM_PATH) ]; then \
		cp $(PYTHON_WASM_PATH) $(FULL_DIR)/data/cache/python-runner/$(PYTHON_WASM_NAME) && \
		echo "Copied Python WASM file to data/cache/python-runner/"; \
	else \
		echo "Warning: Python WASM file not found at $(PYTHON_WASM_PATH)"; \
		echo "Warning: Full version requires Python WASM file. Please build it first:"; \
		echo "  cd python/functionstream-runtime && make build"; \
	fi
	@if [ -f config.yaml ]; then \
		cp config.yaml $(FULL_DIR)/conf/config.yaml && \
		cp config.yaml $(FULL_DIR)/conf/config.yaml.template && \
		echo "Copied config.yaml and config.yaml.template"; \
	else \
		echo "Warning: config.yaml not found, creating default..."; \
		echo "service:" > $(FULL_DIR)/conf/config.yaml; \
		echo "  service_id: \"default-service\"" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  service_name: \"function-stream\"" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  version: \"$(VERSION)\"" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  host: \"127.0.0.1\"" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  port: 8080" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  debug: false" >> $(FULL_DIR)/conf/config.yaml; \
		echo "logging:" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  level: info" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  format: json" >> $(FULL_DIR)/conf/config.yaml; \
		echo "python:" >> $(FULL_DIR)/conf/config.yaml; \
		echo "  wasm_path: data/cache/python-runner/$(PYTHON_WASM_NAME)" >> $(FULL_DIR)/conf/config.yaml; \
		cp $(FULL_DIR)/conf/config.yaml $(FULL_DIR)/conf/config.yaml.template; \
	fi
	@echo "FULL VERSION - $(VERSION)" > $(FULL_DIR)/VERSION.txt
	@echo "Build date: $$(date -u +"%Y-%m-%d %H:%M:%S UTC")" >> $(FULL_DIR)/VERSION.txt
	@echo "Architecture: $(ARCH)" >> $(FULL_DIR)/VERSION.txt
	@echo "OS: $(OS)" >> $(FULL_DIR)/VERSION.txt
	@if [ -f $(PYTHON_WASM_PATH) ]; then \
		echo "Python WASM: Included (data/cache/python-runner/$(PYTHON_WASM_NAME))" >> $(FULL_DIR)/VERSION.txt; \
	fi

prepare-dist-lite:
	@echo "Preparing lite distribution..."
	@if [ -d $(LITE_DIR) ]; then \
		echo "Removing existing lite distribution directory..."; \
		rm -rf $(LITE_DIR); \
	fi
	@mkdir -p $(LITE_DIR)/bin
	@mkdir -p $(LITE_DIR)/conf
	@mkdir -p $(LITE_DIR)/data
	@mkdir -p $(LITE_DIR)/logs
	@if [ -f $(BUILD_DIR)/function-stream ]; then \
		cp $(BUILD_DIR)/function-stream $(LITE_DIR)/bin/ && \
		chmod +x $(LITE_DIR)/bin/function-stream && \
		echo "Copied function-stream binary"; \
	fi
	@if [ -f LICENSE ]; then \
		cp LICENSE $(LITE_DIR)/ && \
		echo "Copied LICENSE file"; \
	else \
		echo "Warning: LICENSE file not found"; \
	fi
	@if [ -f README.md ]; then \
		cp README.md $(LITE_DIR)/ && \
		echo "Copied README.md"; \
	fi
	@if [ -f config.yaml ]; then \
		cp config.yaml $(LITE_DIR)/conf/config.yaml && \
		cp config.yaml $(LITE_DIR)/conf/config.yaml.template && \
		echo "Copied config.yaml and config.yaml.template"; \
	else \
		echo "Warning: config.yaml not found, creating default..."; \
		echo "service:" > $(LITE_DIR)/conf/config.yaml; \
		echo "  service_id: \"default-service\"" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  service_name: \"function-stream\"" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  version: \"$(VERSION)\"" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  host: \"127.0.0.1\"" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  port: 8080" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  debug: false" >> $(LITE_DIR)/conf/config.yaml; \
		echo "logging:" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  level: info" >> $(LITE_DIR)/conf/config.yaml; \
		echo "  format: json" >> $(LITE_DIR)/conf/config.yaml; \
		cp $(LITE_DIR)/conf/config.yaml $(LITE_DIR)/conf/config.yaml.template; \
	fi
	@echo "LITE VERSION - $(VERSION)" > $(LITE_DIR)/VERSION.txt
	@echo "Build date: $$(date -u +"%Y-%m-%d %H:%M:%S UTC")" >> $(LITE_DIR)/VERSION.txt
	@echo "Architecture: $(ARCH)" >> $(LITE_DIR)/VERSION.txt
	@echo "OS: $(OS)" >> $(LITE_DIR)/VERSION.txt
	@echo "Note: Python processor support is disabled" >> $(LITE_DIR)/VERSION.txt

package-full: build-full prepare-dist-full
	@if [ ! -f $(FULL_DIR)/LICENSE ]; then \
		echo "Error: LICENSE file is required for distribution but not found"; \
		echo "Apache License 2.0 requires LICENSE file to be included in distribution"; \
		exit 1; \
	fi
	@if [ ! -f $(FULL_DIR)/data/cache/python-runner/$(PYTHON_WASM_NAME) ]; then \
		echo "Error: Python WASM file is required for full version but not found in distribution"; \
		echo "Please ensure Python WASM is built before packaging"; \
		exit 1; \
	fi
	@echo "Packaging full version..."
	@mkdir -p $(DIST_BASE)/$(PACKAGE_DIR)
	@cd $(DIST_BASE) && \
		zip -r $(PACKAGE_DIR)/function-stream-$(VERSION).zip function-stream-$(VERSION) && \
		tar -czf $(PACKAGE_DIR)/function-stream-$(VERSION).tar.gz function-stream-$(VERSION)
	@echo "Full version packages created:"
	@ls -lh $(DIST_BASE)/$(PACKAGE_DIR)/function-stream-$(VERSION).*

package-lite: build-lite prepare-dist-lite
	@if [ ! -f $(LITE_DIR)/LICENSE ]; then \
		echo "Error: LICENSE file is required for distribution but not found"; \
		echo "Apache License 2.0 requires LICENSE file to be included in distribution"; \
		exit 1; \
	fi
	@echo "Packaging lite version..."
	@mkdir -p $(DIST_BASE)/$(PACKAGE_DIR)
	@cd $(DIST_BASE) && \
		zip -r $(PACKAGE_DIR)/function-stream-$(VERSION)-lite.zip function-stream-$(VERSION)-lite && \
		tar -czf $(PACKAGE_DIR)/function-stream-$(VERSION)-lite.tar.gz function-stream-$(VERSION)-lite
	@echo "Lite version packages created:"
	@ls -lh $(DIST_BASE)/$(PACKAGE_DIR)/function-stream-$(VERSION)-lite.*

package-all: clean-dist package-full package-lite
	@echo ""
	@echo "All packages created:"
	@ls -lh $(DIST_BASE)/$(PACKAGE_DIR)/

docker-build: package-full
	@echo "Building Docker image (using dist/packages/function-stream-$(VERSION).zip)..."
	docker-compose build

docker-up: package-full
	@echo "Building and starting containers..."
	docker-compose up -d --build

test:
	cargo test

install: build-full prepare-dist-full
	@echo "Installation complete"
	@echo "Distribution directory: $(FULL_DIR)"
