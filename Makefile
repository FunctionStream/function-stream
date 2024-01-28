.PHONY: build
build:
	go build -v -o bin/function-stream ./cmd

build_example:
	tinygo build -o bin/example_basic.wasm -target=wasi ./examples/basic

lint:
	golangci-lint run

test: build build_example
	go test ./... -timeout 10m