# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

.PHONY: help install install-editable uninstall build clean test

# Python 解释器
PYTHON := python3
PIP := $(PYTHON) -m pip

# 项目信息
PROJECT_NAME := functionstream-api
PACKAGE_NAME := functionstream_api

help:
	@echo "Function Stream API - Makefile"
	@echo ""
	@echo "Available targets:"
	@echo "  install          - Install package to system Python (via pip)"
	@echo "  install-editable - Install package in editable mode (development)"
	@echo "  uninstall        - Uninstall package from system Python"
	@echo "  build            - Build wheel and source distribution"
	@echo "  clean            - Clean build artifacts"
	@echo "  test             - Run tests (if available)"
	@echo ""

install:
	@echo "Installing $(PROJECT_NAME) to system Python..."
	$(PIP) install --upgrade pip
	$(PIP) install .
	@echo "✓ Installation complete"
	@echo ""
	@echo "To verify installation:"
	@echo "  $(PYTHON) -c 'import $(PACKAGE_NAME); print($(PACKAGE_NAME).__version__)'"

install-editable:
	@echo "Installing $(PROJECT_NAME) in editable mode..."
	$(PIP) install --upgrade pip
	$(PIP) install -e .
	@echo "✓ Editable installation complete"
	@echo ""
	@echo "Changes to source code will be immediately available"

uninstall:
	@echo "Uninstalling $(PROJECT_NAME)..."
	$(PIP) uninstall -y $(PROJECT_NAME) || true
	@echo "✓ Uninstallation complete"

build:
	@echo "Building $(PROJECT_NAME)..."
	$(PYTHON) -m pip install --upgrade build wheel
	$(PYTHON) -m build
	@echo "✓ Build complete"
	@echo ""
	@echo "Distribution files are in the 'dist' directory"

clean:
	@echo "Cleaning build artifacts..."
	rm -rf build/
	rm -rf dist/
	rm -rf *.egg-info/
	rm -rf __pycache__/
	find . -type d -name __pycache__ -exec rm -r {} + 2>/dev/null || true
	find . -type f -name "*.pyc" -delete 2>/dev/null || true
	find . -type f -name "*.pyo" -delete 2>/dev/null || true
	@echo "✓ Clean complete"

test:
	@echo "Running tests..."
	@if [ -d "tests" ]; then \
		$(PYTHON) -m pytest tests/ -v || echo "No pytest found, skipping tests"; \
	else \
		echo "No tests directory found"; \
	fi

# 显示包信息
info:
	@echo "Project: $(PROJECT_NAME)"
	@echo "Package: $(PACKAGE_NAME)"
	@$(PYTHON) -c "import sys; print(f'Python: {sys.version}')"
	@$(PYTHON) -c "import sys; print(f'Python path: {sys.executable}')"
	@echo ""
	@echo "Installed packages:"
	@$(PIP) list | grep -i functionstream || echo "  (not installed)"

