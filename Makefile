.PHONY: build
build:
	go build -v -o bin/function-stream ./cmd

build_example:
	tinygo build -o bin/example_basic.wasm -target=wasi ./examples/basic

lint:
	golangci-lint run

test: build build_example
	go test ./... -timeout 10m

gen_rest_client:
	mkdir -p restclient
	openapi-generator generate -i ./openapi.yaml -g go -o restclient \
		--git-user-id functionstream \
		--git-repo-id functionstream/restclient \
		--package-name restclient \
		--global-property apiDocs,apis,models,supportingFiles
	rm -r restclient/go.mod restclient/go.sum restclient/.travis.yml restclient/.openapi-generator-ignore \
		restclient/git_push.sh restclient/.openapi-generator restclient/api restclient/test
