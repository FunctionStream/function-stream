#!/bin/bash

# Rust WASM Processor 构建脚本
# 将 Rust 代码编译为 WASM Component

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

echo -e "${GREEN}Building Rust WASM Processor Component...${NC}"

# 检查 cargo 是否安装
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: cargo is not installed${NC}"
    echo "Please install Rust: https://www.rust-lang.org/tools/install"
    exit 1
fi

# 检查 wasm32-wasip1 target 是否安装
if ! rustup target list --installed | grep -q "wasm32-wasip1"; then
    echo -e "${YELLOW}Installing wasm32-wasip1 target...${NC}"
    rustup target add wasm32-wasip1
fi

# 输出目录
OUTPUT_DIR="build"
mkdir -p "$OUTPUT_DIR"

# 编译 Rust 代码为 WASM Component
echo -e "${GREEN}Compiling Rust to WASM Component...${NC}"
cargo build --target wasm32-wasip1 --release

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to compile Rust to WASM Component${NC}"
    exit 1
fi

# 复制生成的 WASM 文件
WASM_FILE="target/wasm32-wasip1/release/rust_processor.wasm"
if [ ! -f "$WASM_FILE" ]; then
    # 尝试查找实际的输出文件名
    WASM_FILE=$(find target/wasm32-wasip1/release -name "*.wasm" -type f | head -1)
    if [ -z "$WASM_FILE" ]; then
        echo -e "${RED}Error: Could not find generated WASM file${NC}"
        exit 1
    fi
fi

cp "$WASM_FILE" "$OUTPUT_DIR/processor.wasm"

echo -e "${GREEN}✓ WASM Component built: $OUTPUT_DIR/processor.wasm${NC}"

# 显示文件信息
WASM_SIZE=$(du -h "$OUTPUT_DIR/processor.wasm" | cut -f1)
echo -e "${GREEN}WASM Component size: $WASM_SIZE${NC}"

echo -e "${GREEN}Build completed successfully!${NC}"
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Use the config.yaml to register the task"
echo "2. The WASM Component is located at: $OUTPUT_DIR/processor.wasm"

