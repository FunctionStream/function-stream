# functionstream-runtime

Function Stream Runtime - WASM 内核和 WIT 实现

包含真实 WIT 绑定，只存在于服务端。

## 项目结构

```
functionstream-runtime/
├── fs_runtime/
│   ├── __init__.py
│   ├── wit_impl.py      # WIT 实现 (State/Collector/Factory)
│   ├── kernel.py         # 启动入口 (Inject & Run)
│   └── app.py            # WIT 导出函数（内部实现）
├── main.py               # componentize-py 入口点（WitWorld 类）
├── build.py              # 构建脚本
├── Makefile              # Makefile 构建命令
├── pyproject.toml        # 项目配置
├── target/               # 构建输出目录
│   └── functionstream-runtime.wasm
└── README.md             # 本文档
```

## 安装

### 安装依赖

```bash
# 安装 functionstream-api 包
cd ../functionstream-api
pip install -e .

# 安装 functionstream-runtime 包
cd ../functionstream-runtime
pip install -e .
```

### 安装构建工具

```bash
pip install componentize-py
```

或者使用 Makefile：

```bash
make install-deps
```

## 构建 WASM 组件

### 使用 Python 脚本

```bash
python3 build.py
```

### 使用 Makefile

```bash
make build
```

构建产物会输出到 `target/functionstream-runtime.wasm`。

## 构建要求

- Python 3.7+
- `componentize-py` - Python to WASM 编译器
- `wasm-tools` - WebAssembly 工具链（可选，用于验证）

## 工作原理

1. **kernel_init**: 注入 `WasmFactory`，使 `fs_api.Context` 使用 WIT 能力
2. **kernel_custom**: 反序列化用户代码，`fs_api` 是引用，会使用已注入的工厂
3. **kernel_process**: 运行用户代码，`Context()` 通过工厂获取 WIT 后端

## 清理

```bash
make clean
```

## 许可证

Apache License 2.0

