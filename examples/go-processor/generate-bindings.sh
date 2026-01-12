#!/bin/bash

set -e

echo "生成 Go 绑定代码..."

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
WIT_FILE="$PROJECT_ROOT/wit/processor.wit"
OUT_DIR="$SCRIPT_DIR/bindings"

if [ ! -f "$WIT_FILE" ]; then
    echo "错误: 找不到 WIT 文件: $WIT_FILE"
    exit 1
fi

# 检查 wit-bindgen-go
WIT_BINDGEN_GO=""
if command -v wit-bindgen-go &> /dev/null; then
    WIT_BINDGEN_GO="wit-bindgen-go"
elif [ -f "$HOME/go/bin/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$HOME/go/bin/wit-bindgen-go"
elif [ -n "$GOPATH" ] && [ -f "$GOPATH/bin/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$GOPATH/bin/wit-bindgen-go"
elif [ -n "$GOBIN" ] && [ -f "$GOBIN/wit-bindgen-go" ]; then
    WIT_BINDGEN_GO="$GOBIN/wit-bindgen-go"
fi

if [ -z "$WIT_BINDGEN_GO" ]; then
    echo "安装 wit-bindgen-go..."
    go install go.bytecodealliance.org/cmd/wit-bindgen-go@latest
    if [ -f "$HOME/go/bin/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$HOME/go/bin/wit-bindgen-go"
    elif [ -n "$GOPATH" ] && [ -f "$GOPATH/bin/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$GOPATH/bin/wit-bindgen-go"
    elif [ -n "$GOBIN" ] && [ -f "$GOBIN/wit-bindgen-go" ]; then
        WIT_BINDGEN_GO="$GOBIN/wit-bindgen-go"
    else
        echo "错误: 无法找到 wit-bindgen-go"
        exit 1
    fi
fi

# 创建输出目录
mkdir -p "$OUT_DIR"

# 生成绑定
echo "运行 wit-bindgen-go generate..."
echo "WIT 文件: $WIT_FILE"
echo "输出目录: $OUT_DIR"
echo "World: processor"

"$WIT_BINDGEN_GO" generate "$WIT_FILE" --world="processor" --out="$OUT_DIR"

if [ $? -eq 0 ]; then
    echo "✓ 绑定代码已生成到 $OUT_DIR/ 目录"
    echo ""
    echo "说明:"
    echo "  - 这些绑定文件是中间产物，用于编译 Go 代码"
    echo "  - 最终只会生成一个 WASM 文件: build/processor.wasm"
    echo "  - 绑定文件数量取决于 WIT 文件中定义的接口数量（kv, collector, processor 等）"
    echo ""
    echo "注意: 如果遇到 Go 1.24 wasmimport/wasmexport 兼容性问题，"
    echo "      请检查 wit-bindgen-go 是否有更新版本支持，或考虑使用其他工具。"
else
    echo "✗ 绑定代码生成失败"
    exit 1
fi

