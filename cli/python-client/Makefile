# Makefile for Function Stream Python Client

PROJECT_NAME := functionstream-client
PACKAGE_NAME := fs_client
PROTO_DIR := protos
PROTO_FILE := function_stream.proto
PYTHON_OUT := src/$(PACKAGE_NAME)/_proto

.PHONY: install install-editable uninstall build clean proto test info

install:
	python3 -m pip install .

install-editable:
	python3 -m pip install -e .

uninstall:
	python3 -m pip uninstall -y $(PROJECT_NAME)

build:
	python3 -m build

clean:
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf src/$(PACKAGE_NAME).egg-info/
	rm -rf __pycache__/
	rm -rf src/$(PACKAGE_NAME)/__pycache__/
	rm -rf src/$(PACKAGE_NAME)/_proto/__pycache__/
	rm -rf src/$(PACKAGE_NAME)/*.pyc
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete
	rm -f src/$(PACKAGE_NAME)/_proto/*_pb2.py
	rm -f src/$(PACKAGE_NAME)/_proto/*_pb2_grpc.py
	rm -f src/$(PACKAGE_NAME)/_proto/*_pb2.pyi
	rm -f src/$(PACKAGE_NAME)/_proto/*_pb2_grpc.pyi

proto:
	python3 scripts/codegen.py

test:
	python3 -m pytest tests/

info:
	@echo "Project: $(PROJECT_NAME)"
	@echo "Package: $(PACKAGE_NAME)"
	@echo "Proto file: $(PROTO_DIR)/$(PROTO_FILE)"
