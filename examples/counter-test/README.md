# Counter Test - Kafka 测试工具

用于测试 Go processor 计数器功能的 Kafka 生产者和消费者工具。

## 功能

- **生产者**: 向 `input-topic` 发送测试消息
- **消费者**: 从 `output-topic` 消费并解析计数器结果

## 依赖

- Rust 1.70+
- Kafka broker (运行在 localhost:9092 或通过环境变量配置)
- `rdkafka` 库依赖的系统库（通过 `cmake-build` feature 自动构建）

## 使用方法

### 方式 1: 使用运行脚本（推荐）

```bash
cd examples/counter-test
./run.sh
```

脚本会自动：
1. 检查 Kafka 连接
2. 创建必要的主题（如果不存在）
3. 编译 Rust 程序
4. 运行测试

### 方式 2: 手动运行

```bash
# 编译
cargo build --release

# 运行
./target/release/kafka_test <broker> <input-topic> <output-topic>

# 示例
./target/release/kafka_test localhost:9092 input-topic output-topic
```

### 环境变量

可以通过环境变量配置：

```bash
export KAFKA_BROKER=localhost:9092
export INPUT_TOPIC=input-topic
export OUTPUT_TOPIC=output-topic
./run.sh
```

## 测试流程

1. **生产阶段**: 向 `input-topic` 发送以下测试数据：
   - "apple" (3次)
   - "banana" (2次)
   - "cherry" (1次)

2. **等待处理**: 等待 5 秒让 Go processor 处理消息

3. **消费阶段**: 从 `output-topic` 消费消息，期望收到 JSON 格式的计数器结果：
   ```json
   {
     "apple": 3,
     "banana": 2,
     "cherry": 1
   }
   ```

## 预期输出

当计数器达到 5 的倍数时，Go processor 会输出 JSON 格式的计数器映射。测试工具会：
- 显示接收到的原始消息
- 解析并格式化显示计数器结果

## 故障排除

### Kafka 连接失败

确保 Kafka 正在运行：

```bash
# 使用 Docker Compose 启动
cd examples/go-processor
docker-compose up -d

# 或使用启动脚本
./start-kafka.sh
```

### 编译错误

如果 `rdkafka` 编译失败，确保已安装：
- CMake
- OpenSSL 开发库
- zlib 开发库

macOS:
```bash
brew install cmake openssl zlib
```

Linux (Ubuntu/Debian):
```bash
sudo apt-get install cmake libssl-dev zlib1g-dev
```

### 未收到消息

- 确保 Go processor 正在运行并处理 `input-topic`
- 检查主题是否正确创建
- 增加等待时间（修改 `kafka_test.rs` 中的超时时间）

