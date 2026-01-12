#!/bin/bash

# 启动 Kafka 的便捷脚本
# 使用 Docker Compose 或 Docker 命令启动 Kafka

set -e

echo "正在启动 Kafka..."

# 检查 Docker 是否运行
if ! docker info > /dev/null 2>&1; then
    echo "错误: Docker 未运行，请先启动 Docker"
    exit 1
fi

# 使用 Docker Compose（如果存在 docker-compose.yml）
if [ -f "docker-compose.yml" ]; then
    echo "使用 docker-compose 启动..."
    docker-compose up -d
    echo "✓ Kafka 已启动"
    exit 0
fi

# 使用 Docker 命令启动（如果没有 docker-compose.yml）
echo "使用 Docker 命令启动 Kafka..."

# 检查是否已有运行的容器
if docker ps | grep -q kafka; then
    echo "Kafka 已经在运行"
    exit 0
fi

# 启动 Zookeeper（Kafka 需要）
echo "启动 Zookeeper..."
docker run -d \
    --name zookeeper \
    -p 2181:2181 \
    -e ZOOKEEPER_CLIENT_PORT=2181 \
    confluentinc/cp-zookeeper:latest || {
    if docker ps -a | grep -q zookeeper; then
        echo "Zookeeper 容器已存在，启动中..."
        docker start zookeeper
    else
        echo "错误: 无法启动 Zookeeper"
        exit 1
    fi
}

# 等待 Zookeeper 就绪
echo "等待 Zookeeper 就绪..."
sleep 5

# 启动 Kafka
echo "启动 Kafka..."
docker run -d \
    --name kafka \
    -p 9092:9092 \
    -e KAFKA_BROKER_ID=1 \
    -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 \
    -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
    -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
    --link zookeeper:zookeeper \
    confluentinc/cp-kafka:latest || {
    if docker ps -a | grep -q kafka; then
        echo "Kafka 容器已存在，启动中..."
        docker start kafka
    else
        echo "错误: 无法启动 Kafka"
        exit 1
    fi
}

# 等待 Kafka 就绪
echo "等待 Kafka 就绪..."
sleep 10

# 检查 Kafka 是否运行
if docker ps | grep -q kafka; then
    echo "✓ Kafka 已成功启动在 localhost:9092"
    echo ""
    echo "创建测试主题（如果需要）:"
    echo "  docker exec -it kafka kafka-topics --create --topic input-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
    echo "  docker exec -it kafka kafka-topics --create --topic output-topic --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
else
    echo "✗ Kafka 启动失败"
    exit 1
fi


