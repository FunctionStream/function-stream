# Python WASM Processor 示例

这是一个使用 Python 语言编写的 WASM Component 处理器示例。

## 目录结构

```
python-processor/
├── main.py                  # Python 源代码
├── config.yaml              # 任务配置文件
├── build.sh                 # 构建脚本
├── requirements.txt         # Python 依赖
├── bindings/                # 生成的绑定代码（运行 generate-bindings.sh 后生成）
└── README.md                # 本文件
```

## 前置要求

1. **Python 3.9+**: 用于编写代码
   - 安装方法: https://www.python.org/downloads/
   - 验证: `python3 --version`

2. **componentize-py**: 官方 Python 到 Component Model WASM 工具
   - 项目: https://github.com/bytecodealliance/componentize-py
   - PyPI: https://pypi.org/project/componentize-py/
   - 安装方法（推荐从 PyPI 安装）:
     ```bash
     # 方法1: 从 PyPI 安装（推荐，最快最简单）
     pip install --user componentize-py
     
     # 方法2: 如果 PyPI 不可用，从 GitHub 安装（需要 SSH 密钥）
     pip install --user git+ssh://git@github.com/bytecodealliance/componentize-py.git
     
     # 方法3: 手动克隆安装
     git clone --recursive git@github.com:bytecodealliance/componentize-py.git
     cd componentize-py
     pip install --user .
     ```
   - 验证安装: `componentize-py --version` 或 `python3 -m componentize_py --version`
   - 注意: 
     - 构建脚本优先使用 PyPI 安装（推荐）
     - 如果 PyPI 失败，会自动回退到 GitHub 安装
     - 构建脚本会自动尝试安装

## 构建步骤

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

或者直接安装 componentize-py:

```bash
pip install componentize-py
```

### 2. 构建 WASM Component

```bash
./build.sh
```

构建脚本会自动：
- 检查并安装 componentize-py（如果未安装）
- 使用 componentize-py 将 Python 代码编译为 Component Model WASM
- 输出 `build/processor.wasm` 文件

## 功能说明

这个 Python processor 实现了以下功能：

1. **计数器**: 统计每个输入字符串的出现次数
2. **状态存储**: 使用 KV store 持久化计数器状态
3. **批量输出**: 当计数达到 5 的倍数时，输出 JSON 格式的计数器结果

## 处理器接口

处理器实现了以下 WIT 接口：

- `init(config)`: 初始化处理器
- `process(source_id, data)`: 处理输入数据
- `process_watermark(source_id, watermark)`: 处理水位线
- `take_checkpoint(checkpoint_id)`: 创建检查点
- `check_heartbeat()`: 健康检查
- `close()`: 关闭处理器
- `exec_custom(payload)`: 执行自定义命令

## 使用示例

1. **构建处理器**:
   ```bash
   cd examples/python-processor
   ./build.sh
   ```

2. **注册任务** (通过 SQL):
   ```sql
   CREATE WASMTASK python-processor-example WITH (
       'wasm-path'='/path/to/examples/python-processor/build/processor.wasm',
       'config-path'='/path/to/examples/python-processor/config.yaml'
   );
   ```

3. **启动任务**:
   ```sql
   START WASMTASK python-processor-example;
   ```

## 注意事项

✅ **使用官方 componentize-py 工具**:

- `componentize-py` 是 Bytecode Alliance 官方提供的 Python 到 Component Model 工具
- 直接支持 Component Model，无需额外转换步骤
- 自动生成 WIT 绑定代码
- 支持标准的 Python 代码和常用库

## 故障排除

### 绑定生成失败

如果 `wit-bindgen-python` 不可用，可能需要：
- 手动编写绑定代码
- 使用其他 Python WASM 运行时
- 考虑使用 Go 或 Rust 替代

### WASM 编译失败

确保已安装：
- Pyodide 或相应的 Python-to-WASM 工具
- wasm-tools（用于 Component Model 转换）

### 运行时错误

检查：
- Python 版本兼容性
- 依赖项是否正确安装
- WASM 运行时环境配置

## 与 Go/Rust 示例的区别

- **语言特性**: Python 的动态类型和易用性
- **工具链**: 使用官方 componentize-py，支持完整 Component Model
- **性能**: 解释执行，通常比编译型语言（Go/Rust）慢，但适合快速开发
- **适用场景**: 适合快速原型开发和 Python 生态系统的集成

