<!--

    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

        http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

-->

# 连接器、数据格式与 SQL 类型参考

[中文](connectors-and-formats-zh.md) | [English](connectors-and-formats.md)

本文档是 Function Stream Streaming SQL 引擎所支持的连接器（Source / Sink）、序列化格式以及 SQL 数据类型的权威参考。

---

## 目录

- [1. 连接器 (Connector)](#1-连接器-connector)
  - [1.1 Kafka Source（数据源）](#11-kafka-source数据源)
  - [1.2 Kafka Sink（数据汇）](#12-kafka-sink数据汇)
- [2. 数据格式 (Format)](#2-数据格式-format)
- [3. SQL 数据类型](#3-sql-数据类型)
- [4. 完整示例](#4-完整示例)

---

## 1. 连接器 (Connector)

当前 Function Stream 支持 **Kafka** 作为生产可用的连接器，同时可作为数据源（Source）和数据汇（Sink）。

### 1.1 Kafka Source（数据源）

Kafka Source 从一个或多个 Kafka Topic 分区读取消息。在 `CREATE TABLE` 中使用以注册输入流。

**必填属性：**

| 属性 | 说明 | 示例 |
|------|------|------|
| `connector` | 必须为 `kafka`。 | `'kafka'` |
| `topic` | 要消费的 Kafka Topic。 | `'raw_events'` |
| `format` | 消息的序列化格式。 | `'json'` |
| `bootstrap.servers` | Kafka Broker 地址列表，逗号分隔。 | `'broker1:9092,broker2:9092'` |

**示例：**

```sql
CREATE TABLE page_views (
    user_id VARCHAR,
    page_url VARCHAR,
    view_time TIMESTAMP NOT NULL,
    WATERMARK FOR view_time AS view_time - INTERVAL '3' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'page_views',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
```

### 1.2 Kafka Sink（数据汇）

Kafka Sink 将计算结果写入 Kafka Topic。在 `CREATE STREAMING TABLE` 的 `WITH` 子句中配置。

**必填属性：**

| 属性 | 说明 | 示例 |
|------|------|------|
| `connector` | 必须为 `kafka`。 | `'kafka'` |
| `topic` | 要写入的 Kafka Topic。 | `'sink_results'` |
| `format` | 输出消息的序列化格式。 | `'json'` |
| `bootstrap.servers` | Kafka Broker 地址列表。 | `'broker1:9092'` |

**示例：**

```sql
CREATE STREAMING TABLE enriched_clicks WITH (
    'connector' = 'kafka',
    'topic' = 'enriched_clicks',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT click_id, user_id, click_time
FROM ad_clicks;
```

---

## 2. 数据格式 (Format)

当前唯一支持的序列化格式是 **JSON**。每条 Kafka 消息应为一个自描述的 JSON 对象，其字段直接映射到 `CREATE TABLE` 中定义的列。

在 `WITH` 子句中设置 `'format' = 'json'`（省略时也默认为 JSON）。

---

## 3. SQL 数据类型

以下是 `CREATE TABLE` 列定义中支持的 SQL 数据类型：

### 数值类型

| SQL 类型 | 别名 | Arrow 类型 | 说明 |
|----------|------|-----------|------|
| `BOOLEAN` | `BOOL` | Boolean | 布尔值。 |
| `TINYINT` | — | Int8 | 8 位有符号整数。 |
| `SMALLINT` | `INT2` | Int16 | 16 位有符号整数。 |
| `INT` | `INTEGER`、`INT4` | Int32 | 32 位有符号整数。 |
| `BIGINT` | `INT8` | Int64 | 64 位有符号整数。 |
| `TINYINT UNSIGNED` | — | UInt8 | 8 位无符号整数。 |
| `SMALLINT UNSIGNED` | `INT2 UNSIGNED` | UInt16 | 16 位无符号整数。 |
| `INT UNSIGNED` | `INT4 UNSIGNED` | UInt32 | 32 位无符号整数。 |
| `BIGINT UNSIGNED` | `INT8 UNSIGNED` | UInt64 | 64 位无符号整数。 |
| `FLOAT` | `REAL`、`FLOAT4` | Float32 | 32 位 IEEE 754 浮点数。 |
| `DOUBLE` | `DOUBLE PRECISION`、`FLOAT8` | Float64 | 64 位 IEEE 754 浮点数。 |
| `DECIMAL(p, s)` | `NUMERIC(p, s)` | Decimal128 | 定点小数。精度 1–38，标度 <= 精度。 |

### 字符串与二进制类型

| SQL 类型 | 别名 | Arrow 类型 | 说明 |
|----------|------|-----------|------|
| `VARCHAR` | `TEXT`、`STRING`、`CHAR` | Utf8 | 可变长度 UTF-8 字符串。 |
| `BYTEA` | — | Binary | 可变长度字节数组。 |
| `JSON` | — | Utf8（JSON 扩展） | 带有 FunctionStream 扩展元数据的 JSON 类型字符串。 |

### 日期与时间类型

| SQL 类型 | Arrow 类型 | 说明 |
|----------|-----------|------|
| `TIMESTAMP` | Timestamp(Nanosecond) | 不含时区的日期时间（纳秒精度）。 |
| `TIMESTAMP(0)` | Timestamp(Second) | 秒精度。 |
| `TIMESTAMP(3)` | Timestamp(Millisecond) | 毫秒精度。 |
| `TIMESTAMP(6)` | Timestamp(Microsecond) | 微秒精度。 |
| `TIMESTAMP(9)` | Timestamp(Nanosecond) | 纳秒精度（与 `TIMESTAMP` 相同）。 |
| `DATE` | Date32 | 日历日期（年、月、日）。 |
| `DATETIME` | Timestamp(Nanosecond) | `TIMESTAMP` 的别名。 |
| `TIME` | Time64(Nanosecond) | 不含时区的时刻。 |
| `INTERVAL` | Interval(MonthDayNano) | 时间间隔 / 持续时间。 |

### 复合类型

| SQL 类型 | Arrow 类型 | 说明 |
|----------|-----------|------|
| `STRUCT<name type, ...>` | Struct | 命名组合字段。 |
| `ARRAY<element_type>` | List | 相同类型元素的有序列表。也支持 `element_type[]` 语法。 |

---

## 4. 完整示例

以下是一个结合 Kafka Source、Kafka Sink、JSON 格式和多种 SQL 数据类型的完整示例：

```sql
-- Source：从 Kafka 读取用户活动事件
CREATE TABLE user_activity (
    event_id VARCHAR,
    user_id BIGINT,
    action VARCHAR,
    amount DECIMAL(10, 2),
    tags ARRAY<VARCHAR>,
    event_time TIMESTAMP NOT NULL,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_activity',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- Sink：1 分钟滚动窗口聚合
CREATE STREAMING TABLE activity_stats_1m WITH (
    'connector' = 'kafka',
    'topic' = 'activity_stats_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    TUMBLE(INTERVAL '1' MINUTE) AS time_window,
    action,
    COUNT(*) AS event_count,
    SUM(amount) AS total_amount
FROM user_activity
GROUP BY 1, action;
```
