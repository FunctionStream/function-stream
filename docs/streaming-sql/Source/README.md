# Streaming SQL Source Docs

Create a source table with `CREATE TABLE ... WITH (connector='...', type='source', ...)`, then reference it in `CREATE STREAMING TABLE ... AS SELECT ...`.

## Documentation

### Overview

- [Robot & industrial data sources](robot-sources.md)

### Connectors

| Connector | Doc (EN) | Doc (中文) |
|-----------|----------|------------|
| Kafka | [kafka-source.md](kafka-source.md) | [kafka-source-zh.md](kafka-source-zh.md) |
| MQTT | — | [mqtt-source-zh.md](mqtt-source-zh.md) |
| HTTP | — | [http-source-zh.md](http-source-zh.md) |
| ROS (ROS1) | — | [ros-source-zh.md](ros-source-zh.md) |
| ROS2 | — | [ros2-source-zh.md](ros2-source-zh.md) |
| Robot bag | [robot-bag-source.md](robot-bag-source.md) | [robot-bag-source-zh.md](robot-bag-source-zh.md) |

中文总览：[README-zh.md](README-zh.md)
