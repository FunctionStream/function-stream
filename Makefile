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

.PHONY: license
build:
	go build -v -o bin/fs ./cmd

build-example:
	tinygo build -o bin/example_basic.wasm -target=wasi ./examples/basic
	go build -o bin/example_external_function ./examples/basic

run-example-external-functions:
	FS_SOCKET_PATH=/tmp/fs.sock FS_FUNCTION_NAME=fs/external-function ./bin/example_external_function

lint:
	golangci-lint run

lint-fix:
	golangci-lint run --fix

build-all: build build-example

test:
	go test -race ./... -timeout 10m

bench:
	go test -bench=. ./benchmark -timeout 10m

bench_race:
	go test -race -bench=. ./benchmark -timeout 10m

get-apidocs:
	curl -o apidocs.json http://localhost:7300/apidocs

ADMIN_CLIENT_DIR := admin/client
FILES_TO_REMOVE := go.mod go.sum .travis.yml .openapi-generator-ignore git_push.sh .openapi-generator api test

gen-rest-client:
	-rm -r $(ADMIN_CLIENT_DIR)
	mkdir -p $(ADMIN_CLIENT_DIR)
	openapi-generator generate -i ./apidocs.json -g go -o $(ADMIN_CLIENT_DIR) \
		--git-user-id functionstream \
		--git-repo-id function-stream/$(ADMIN_CLIENT_DIR) \
		--package-name adminclient \
		--global-property apiDocs,apis,models,supportingFiles
	rm -r $(addprefix $(ADMIN_CLIENT_DIR)/, $(FILES_TO_REMOVE))

proto:
	for PROTO_FILE in $$(find . -name '*.proto'); do \
		echo "generating codes for $$PROTO_FILE"; \
		protoc \
			--go_out=. \
			--go_opt paths=source_relative \
			--plugin protoc-gen-go="${GOPATH}/bin/protoc-gen-go" \
			--go-grpc_out=. \
			--go-grpc_opt paths=source_relative \
			--plugin protoc-gen-go-grpc="${GOPATH}/bin/protoc-gen-go-grpc" \
			$$PROTO_FILE; \
	done

license:
	./license-checker/license-checker.sh

gen-changelog:
	.chglog/gen-chg-log.sh
