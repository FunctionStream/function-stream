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


APP_NAME    := function-stream
VERSION     := $(shell grep '^version' Cargo.toml | head -1 | awk -F '"' '{print $$2}')
ARCH        := $(shell uname -m)
OS          := $(shell uname -s | tr '[:upper:]' '[:lower:]')
DATE        := $(shell date -u +"%Y-%m-%dT%H:%M:%SZ")

DIST_ROOT   := dist
TARGET_DIR  := target/release
PYTHON_ROOT := python
WASM_SOURCE := $(PYTHON_ROOT)/functionstream-runtime/target/functionstream-python-runtime.wasm

PYTHON_EXEC := $(if $(wildcard .venv/bin/python),.venv/bin/python,python3)

FULL_NAME   := $(APP_NAME)-$(VERSION)
LITE_NAME   := $(APP_NAME)-$(VERSION)-lite
FULL_PATH   := $(DIST_ROOT)/$(FULL_NAME)
LITE_PATH   := $(DIST_ROOT)/$(LITE_NAME)

DOCKER_REPO := functionstream
DOCKER_TAG  := $(VERSION)
IMAGE_NAME  := $(DOCKER_REPO):$(DOCKER_TAG)

C_R := \033[0;31m
C_G := \033[0;32m
C_B := \033[0;34m
C_Y := \033[0;33m
C_0 := \033[0m

log = @printf "$(C_B)[-]$(C_0) %-15s %s\n" "$(1)" "$(2)"
success = @printf "$(C_G)[✔]$(C_0) %s\n" "$(1)"

.PHONY: all help build build-lite dist dist-lite clean test env env-clean go-sdk-env go-sdk-build go-sdk-clean docker docker-run docker-push .check-env .build-wasm integration-test

all: build

help:
	@echo "Usage: make [TARGET]"
	@echo ""
	@echo "  build       Build full release (Rust + Python WASM)"
	@echo "  build-lite  Build lightweight release (Rust only)"
	@echo "  dist        Package full build (.tar.gz / .zip)"
	@echo "  dist-lite   Package lite build (.tar.gz / .zip)"
	@echo "  env         Setup Python dev environment (.venv)"
	@echo "  go-sdk-env  Setup Go SDK toolchain"
	@echo "  go-sdk-build Copy WIT, generate bindings, build Go SDK"
	@echo "  go-sdk-clean Remove generated Go SDK artifacts"
	@echo "  test        Run unit tests"
	@echo "  clean       Cleanup all artifacts"
	@echo "  docker      Build Docker image"
	@echo "  docker-run  Run container (port 8080, mount logs)"
	@echo "  docker-push Push image to registry"
	@echo ""
	@echo "  integration-test   Run integration tests (delegates to tests/integration)"
	@echo ""
	@echo "  Version: $(VERSION) | Arch: $(ARCH) | OS: $(OS)"

build: .check-env .build-wasm
	$(call log,BUILD,Rust Full Features)
	@cargo build --release --features python --quiet
	$(call log,BUILD,CLI)
	@cargo build --release -p function-stream-cli --quiet
	$(call success,Target: $(TARGET_DIR)/$(APP_NAME) $(TARGET_DIR)/cli)

build-lite: .check-env
	$(call log,BUILD,Rust Lite No Python)
	@cargo build --release --no-default-features --features incremental-cache --quiet
	$(call log,BUILD,CLI for dist)
	@cargo build --release -p function-stream-cli --quiet
	$(call success,Target: $(TARGET_DIR)/$(APP_NAME) $(TARGET_DIR)/cli)

.build-wasm:
	$(call log,WASM,Building Python Runtime using $(PYTHON_EXEC))
	@cd $(PYTHON_ROOT)/functionstream-runtime && \
		PYTHONPATH=../functionstream-api:../functionstream-api-advanced ../../$(PYTHON_EXEC) build.py > /dev/null
	@[ -f "$(WASM_SOURCE)" ] || (printf "$(C_R)[X] WASM Build Failed$(C_0)\n" && exit 1)

dist: build
	$(call log,DIST,Layout: $(FULL_PATH))
	@rm -rf "$(FULL_PATH)"
	@mkdir -p "$(FULL_PATH)/bin" "$(FULL_PATH)/conf" "$(FULL_PATH)/logs" "$(FULL_PATH)/data/cache/python-runner"
	@cp "$(TARGET_DIR)/$(APP_NAME)" "$(FULL_PATH)/bin/"
	@cp "$(TARGET_DIR)/cli" "$(FULL_PATH)/bin/"
	@cp scripts/start-server.sh scripts/start-cli.sh "$(FULL_PATH)/bin/"
	@cp "$(WASM_SOURCE)" "$(FULL_PATH)/data/cache/python-runner/"
	@cp conf/config.yaml "$(FULL_PATH)/conf/config.yaml" 2>/dev/null || true
	@cp LICENSE "$(FULL_PATH)/" 2>/dev/null || true
	@printf "Version: $(VERSION)\nBuild: $(ARCH)-$(OS)\nDate: $(DATE)\n" > "$(FULL_PATH)/manifest.txt"
	$(call log,ARCHIVE,Compressing to $(DIST_ROOT)/...)
	@cd $(DIST_ROOT) && tar -czf "$(FULL_NAME).tar.gz" "$(FULL_NAME)"
	@cd $(DIST_ROOT) && zip -rq "$(FULL_NAME).zip" "$(FULL_NAME)"
	$(call success,Ready: $(DIST_ROOT)/$(FULL_NAME).tar.gz)

dist-lite: build-lite
	$(call log,DIST,Layout: $(LITE_PATH))
	@rm -rf "$(LITE_PATH)"
	@mkdir -p "$(LITE_PATH)/bin" "$(LITE_PATH)/conf" "$(LITE_PATH)/logs" "$(LITE_PATH)/data"
	@cp "$(TARGET_DIR)/$(APP_NAME)" "$(LITE_PATH)/bin/"
	@cp "$(TARGET_DIR)/cli" "$(LITE_PATH)/bin/"
	@cp scripts/start-server.sh scripts/start-cli.sh "$(LITE_PATH)/bin/"
	@cp conf/config.yaml "$(LITE_PATH)/conf/config.yaml" 2>/dev/null || true
	@cp LICENSE "$(LITE_PATH)/" 2>/dev/null || true
	@printf "Version: $(VERSION) (Lite)\nBuild: $(ARCH)-$(OS)\nDate: $(DATE)\n" > "$(LITE_PATH)/manifest.txt"
	$(call log,ARCHIVE,Compressing to $(DIST_ROOT)/...)
	@cd $(DIST_ROOT) && tar -czf "$(LITE_NAME).tar.gz" "$(LITE_NAME)"
	@cd $(DIST_ROOT) && zip -rq "$(LITE_NAME).zip" "$(LITE_NAME)"
	$(call success,Ready: $(DIST_ROOT)/$(LITE_NAME).tar.gz)

test:
	$(call log,TEST,Running cargo tests)
	@cargo test --quiet

env:
	$(call log,ENV,Initializing Python environment)
	@./scripts/setup.sh
	$(call success,Environment Ready)

go-sdk-build:
	$(call log,GO,Building Go SDK)
	@$(MAKE) -C go-sdk build
	$(call success,Go SDK build complete)

go-sdk-clean:
	$(call log,GO,Cleaning Go SDK generated artifacts)
	@$(MAKE) -C go-sdk clean
	$(call success,Go SDK artifacts removed)

env-clean:
	$(call log,CLEAN,Python artifacts)
	@./scripts/clean.sh
	$(call success,Done)

clean:
	$(call log,CLEAN,Removing all artifacts)
	@cargo clean
	@rm -rf $(DIST_ROOT) data logs
	@./scripts/clean.sh 2>/dev/null || true
	@$(MAKE) -C tests/integration clean 2>/dev/null || true
	$(call success,Done)

.check-env:
	@command -v cargo >/dev/null 2>&1 || { printf "$(C_R)[X] Cargo not found$(C_0)\n"; exit 1; }
	@command -v python3 >/dev/null 2>&1 || { printf "$(C_R)[X] Python3 not found$(C_0)\n"; exit 1; }

docker:
	$(call log,DOCKER,Building Image: $(IMAGE_NAME))
	@docker build -t $(IMAGE_NAME) .
	$(call success,Image Ready: $(IMAGE_NAME))

docker-run:
	$(call log,DOCKER,Starting Container)
	@docker run --rm -it \
		-p 8080:8080 \
		-v $(CURDIR)/logs:/app/logs \
		$(IMAGE_NAME)

docker-push:
	$(call log,DOCKER,Pushing $(IMAGE_NAME))
	@docker push $(IMAGE_NAME)
	$(call success,Push Complete)

integration-test:
	@$(MAKE) -C tests/integration test PYTEST_ARGS="$(PYTEST_ARGS)"
