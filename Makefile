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
	go build -v -o bin/function-stream ./cmd

build_example:
	tinygo build -o bin/example_basic.wasm -target=wasi ./examples/basic

lint:
	golangci-lint run

build_all: build build_example

test:
	go test -race ./... -timeout 10m

bench:
	go test -bench=. ./benchmark -timeout 10m

bench_race:
	go test -race -bench=. ./benchmark -timeout 10m

get-apidocs:
	curl -o apidocs.json http://localhost:7300/apidocs

gen-rest-client:
	mkdir -p restclient
	openapi-generator generate -i ./apidocs.json -g go -o restclient \
		--git-user-id functionstream \
		--git-repo-id functionstream/restclient \
		--package-name restclient \
		--global-property apiDocs,apis,models,supportingFiles
	rm -r restclient/go.mod restclient/go.sum restclient/.travis.yml restclient/.openapi-generator-ignore \
		restclient/git_push.sh restclient/.openapi-generator restclient/api restclient/test

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
