#!/bin/bash

# Kafka 测试运行脚本
# 用于测试 Go processor 的计数器功能

set -e

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# 默认配置
BROKER="${KAFKA_BROKER:-localhost:9092}"
INPUT_TOPIC="${INPUT_TOPIC:-input-topic}"
OUTPUT_TOPIC="${OUTPUT_TOPIC:-output-topic}"

echo -e "${GREEN}=== Kafka 计数器测试工具 ===${NC}"
echo "Broker: $BROKER"
echo "Input Topic: $INPUT_TOPIC"
echo "Output Topic: $OUTPUT_TOPIC"
echo ""

# 检查 Kafka 连接
echo -e "${YELLOW}检查 Kafka 连接...${NC}"
if ! timeout 5 bash -c "echo > /dev/tcp/${BROKER%:*}/${BROKER#*:}" 2>/dev/null; then
    echo -e "${RED}错误: 无法连接到 Kafka broker $BROKER${NC}"
    echo "请确保 Kafka 正在运行。可以使用以下命令启动:"
    echo "  cd examples/go-processor && ./start-kafka.sh"
    exit 1
fi
echo -e "${GREEN}✓ Kafka 连接正常${NC}"

# 检查是否有 kafka-topics 命令（用于创建主题）
if command -v kafka-topics &> /dev/null; then
    echo -e "${YELLOW}创建主题（如果不存在）...${NC}"
    kafka-topics --create --if-not-exists \
        --bootstrap-server "$BROKER" \
        --topic "$INPUT_TOPIC" \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
    
    kafka-topics --create --if-not-exists \
        --bootstrap-server "$BROKER" \
        --topic "$OUTPUT_TOPIC" \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
    
    echo -e "${GREEN}✓ 主题已准备${NC}"
elif docker ps | grep -q kafka; then
    echo -e "${YELLOW}使用 Docker 创建主题...${NC}"
    docker exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic "$INPUT_TOPIC" \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
    
    docker exec kafka kafka-topics --create --if-not-exists \
        --bootstrap-server localhost:9092 \
        --topic "$OUTPUT_TOPIC" \
        --partitions 1 \
        --replication-factor 1 2>/dev/null || true
    
    echo -e "${GREEN}✓ 主题已准备${NC}"
else
    echo -e "${YELLOW}警告: 未找到 kafka-topics 命令，假设主题已存在${NC}"
fi

# 编译 Rust 程序
echo -e "${YELLOW}编译测试程序...${NC}"
cd "$(dirname "$0")"
cargo build --release 2>&1 | tail -5

if [ ! -f "target/release/kafka_test" ]; then
    echo -e "${RED}错误: 编译失败${NC}"
    exit 1
fi
echo -e "${GREEN}✓ 编译成功${NC}"

# 运行测试
echo ""
echo -e "${GREEN}=== 开始运行测试 ===${NC}"
echo ""

./target/release/kafka_test "$BROKER" "$INPUT_TOPIC" "$OUTPUT_TOPIC"

echo ""
echo -e "${GREEN}=== 测试完成 ===${NC}"

# 可选：清理主题（注释掉以避免误删）
# echo ""
# echo -e "${YELLOW}清理测试主题...${NC}"
# kafka-topics --delete --bootstrap-server "$BROKER" --topic "$INPUT_TOPIC" 2>/dev/null || true
# kafka-topics --delete --bootstrap-server "$BROKER" --topic "$OUTPUT_TOPIC" 2>/dev/null || true

