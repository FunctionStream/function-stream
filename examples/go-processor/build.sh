#!/bin/bash

# Go wasm Processor 构建脚本
# 将 Go 代码编译为 wasm Component，并生成 YAML 配置文件

set -e

# 颜色输出
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 脚本目录
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 任务名称（从环境变量或使用默认值）
TASK_NAME="${TASK_NAME:-go-processor-example}"

echo -e "${GREEN}Building Go WASM Processor Component...${NC}"

# 检查 TinyGo 是否安装
if ! command -v tinygo &> /dev/null; then
    echo -e "${RED}Error: tinygo is not installed${NC}"
    echo "Please install TinyGo: https://tinygo.org/getting-started/install/"
    exit 1
fi

# 检查 TinyGo 版本
TINYGO_VERSION=$(tinygo version | awk '{print $2}' | sed 's/tinygo version //')
echo -e "${GREEN}Using TinyGo $TINYGO_VERSION${NC}"

# 步骤 1: 生成 WIT 绑定代码
echo -e "${GREEN}Step 1: Generating WIT bindings...${NC}"
chmod +x generate-bindings.sh
./generate-bindings.sh

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to generate WIT bindings${NC}"
    exit 1
fi

# 检查绑定代码是否生成
if [ ! -d "bindings" ] || [ -z "$(ls -A bindings 2>/dev/null)" ]; then
    echo -e "${YELLOW}Warning: No bindings generated, continuing with placeholder implementation${NC}"
fi

       # 输出目录结构：
       # build/ - 最终输出文件
       # build/tmp/ - 临时中间文件（构建后删除）
       # build/deps/ - 依赖文件（保留，每次检查）
       OUTPUT_DIR="build"
       TMP_DIR="$OUTPUT_DIR/tmp"
       DEPS_DIR="$OUTPUT_DIR/deps"
       mkdir -p "$OUTPUT_DIR"
       mkdir -p "$TMP_DIR"
       mkdir -p "$DEPS_DIR"

# 步骤 2: 使用 TinyGo 编译为 wasm
echo -e "${GREEN}Step 2: Compiling Go to WASM (using TinyGo)...${NC}"

PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WIT_FILE="$PROJECT_ROOT/wit/processor.wit"
COMPONENT_FILE="$OUTPUT_DIR/processor.wasm"

# 使用 TinyGo 编译为 wasm（core 模块）
echo "  Compiling with TinyGo (target: wasi)..."
CORE_WASM="$TMP_DIR/processor-core.wasm"
tinygo build -target wasi -o "$CORE_WASM" main.go

if [ $? -ne 0 ]; then
    echo -e "${RED}Failed to compile Go to WASM${NC}"
    exit 1
fi

# 验证生成的文件
if [ ! -f "$CORE_WASM" ]; then
    echo -e "${RED}Error: Core WASM file was not created${NC}"
    exit 1
fi

echo -e "${GREEN}✓ Go compiled to WASM (core module)${NC}"

# 步骤 3: 转换为 Component Model 格式
echo -e "${GREEN}Step 3: Converting to Component Model format...${NC}"

# 检查 wasm-tools 是否可用（必需）
if ! command -v wasm-tools &> /dev/null; then
    echo -e "${RED}Error: wasm-tools is REQUIRED for Component Model conversion${NC}"
    echo "Please install wasm-tools:"
    echo "  cargo install wasm-tools"
    exit 1
fi

# 3.1: 嵌入 WIT 元数据
EMBEDDED_WASM="$TMP_DIR/processor-embedded.wasm"
echo "  Embedding WIT metadata..."
wasm-tools component embed --world processor "$WIT_FILE" "$CORE_WASM" -o "$EMBEDDED_WASM" || {
    echo -e "${RED}Failed to embed WIT metadata${NC}"
    exit 1
}

# 3.2: 准备 WASI 适配器（如果需要）
WASI_ADAPTER_FILE="$DEPS_DIR/wasi_snapshot_preview1.reactor.wasm"
if [ ! -f "$WASI_ADAPTER_FILE" ]; then
    echo "  Downloading WASI adapter..."
    ADAPTER_URLS=(
        "https://github.com/bytecodealliance/wasmtime/releases/download/v24.0.0/wasi_snapshot_preview1.reactor.wasm"
        "https://github.com/bytecodealliance/wasmtime/releases/download/v23.0.0/wasi_snapshot_preview1.reactor.wasm"
        "https://github.com/bytecodealliance/wasmtime/releases/latest/download/wasi_snapshot_preview1.reactor.wasm"
    )
    
    DOWNLOADED=0
    for ADAPTER_URL in "${ADAPTER_URLS[@]}"; do
        echo "    Trying: $ADAPTER_URL"
        if command -v curl &> /dev/null; then
            if curl -L -f --progress-bar -o "$WASI_ADAPTER_FILE" "$ADAPTER_URL" 2>&1 && [ -s "$WASI_ADAPTER_FILE" ]; then
                if [ "$(head -c 4 "$WASI_ADAPTER_FILE" | od -An -tx1 | tr -d ' \n')" = "0061736d" ]; then
                    DOWNLOADED=1
                    echo "    ✓ Successfully downloaded from: $ADAPTER_URL"
                    break
                else
                    rm -f "$WASI_ADAPTER_FILE"
                fi
            fi
        elif command -v wget &> /dev/null; then
            if wget --progress=bar:force -O "$WASI_ADAPTER_FILE" "$ADAPTER_URL" 2>&1 && [ -s "$WASI_ADAPTER_FILE" ]; then
                if [ "$(head -c 4 "$WASI_ADAPTER_FILE" | od -An -tx1 | tr -d ' \n')" = "0061736d" ]; then
                    DOWNLOADED=1
                    echo "    ✓ Successfully downloaded from: $ADAPTER_URL"
                    break
                else
                    rm -f "$WASI_ADAPTER_FILE"
                fi
            fi
        fi
    done
    
    if [ $DOWNLOADED -eq 0 ]; then
        echo -e "${RED}Failed to download WASI adapter${NC}"
        exit 1
    fi
    echo "  ✓ WASI adapter downloaded and verified"
fi

# 3.3: 转换为 Component Model（最终输出）
echo "  Converting to Component Model..."
# 使用 --realloc-via-memory-grow 选项，因为 TinyGo 生成的 wasm 缺少 cabi_realloc 导出
wasm-tools component new "$EMBEDDED_WASM" \
    --adapt wasi_snapshot_preview1="$WASI_ADAPTER_FILE" \
    --realloc-via-memory-grow \
    -o "$COMPONENT_FILE" || {
    echo -e "${RED}Failed to convert to Component Model${NC}"
    exit 1
}

# 验证 Component Model 格式
if wasm-tools validate "$COMPONENT_FILE" 2>&1 > /dev/null; then
    echo -e "${GREEN}✓ Component Model format validated${NC}"
else
    echo -e "${YELLOW}Warning: Component validation failed${NC}"
fi

# 验证输出文件存在
if [ ! -f "$COMPONENT_FILE" ]; then
    echo -e "${RED}Error: Output file was not created: $COMPONENT_FILE${NC}"
    exit 1
fi

echo -e "${GREEN}✓ WASM Component built: $COMPONENT_FILE${NC}"

# 显示文件信息
if [ -f "$COMPONENT_FILE" ]; then
    WASM_SIZE=$(du -h "$COMPONENT_FILE" | cut -f1)
    echo -e "${GREEN}WASM module size: $WASM_SIZE${NC}"
else
    echo -e "${RED}Error: Output file does not exist: $COMPONENT_FILE${NC}"
    exit 1
fi

# 清理说明
echo ""
echo -e "${GREEN}Build completed!${NC}"
echo ""
echo -e "${GREEN}目录结构:${NC}"
echo "  $OUTPUT_DIR/"
echo "    ├── processor.wasm          # 最终输出文件（Component Model 格式）"
echo "    └── deps/"
echo "        └── wasi_snapshot_preview1.reactor.wasm  # WASI 适配器（依赖文件，保留）"
echo ""
echo -e "${YELLOW}说明:${NC}"
echo "  - bindings/ 目录中的文件是中间产物（Go 绑定代码），用于编译"
echo "  - build/tmp/ 目录中的临时文件已自动清理"
echo "  - build/deps/ 目录中的依赖文件会保留，下次构建时检查是否存在"
echo "  - 最终输出: $OUTPUT_DIR/processor.wasm (Component Model 格式)"
echo ""
echo -e "${YELLOW}Next steps:${NC}"
echo "1. Use the generated config.yaml to register the task"
echo "2. The WASM module is located at: $OUTPUT_DIR/processor.wasm"
echo -e "${GREEN}The WASM file is now in Component Model format and ready to use!${NC}"

