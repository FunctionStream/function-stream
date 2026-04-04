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

# Streaming SQL 使用指南

[中文](streaming-sql-guide-zh.md) | [English](streaming-sql-guide.md)

Function Stream 提供了声明式 SQL 接口来构建实时流处理管道。通过 Streaming SQL，您可以轻松应对无界数据流（Unbounded Data）的摄取、时间窗口聚合、流式关联以及任务生命周期管理 — 无需编写任何命令式代码。

---

## 目录

- [核心概念](#核心概念)
- [第一部分：注册数据源 (TABLE)](#第一部分注册数据源-table)
- [第二部分：构建实时 Pipeline (STREAMING TABLE)](#第二部分构建实时-pipeline-streaming-table)
  - [滚动窗口 (Tumbling Window)](#场景-1滚动窗口-tumbling-window)
  - [滑动窗口 (Hopping Window)](#场景-2滑动窗口-hopping-window)
  - [会话窗口 (Session Window)](#场景-3会话窗口-session-window)
  - [窗口双流关联 (Window Join)](#场景-4窗口双流关联-window-join)
- [第三部分：生命周期与流任务管理](#第三部分生命周期与流任务管理)
  - [数据源管理](#1-数据源与元数据管理)
  - [Pipeline 监控](#2-实时-pipeline-监控与排障)
  - [停止与释放](#3-安全停止与释放资源)
- [SQL 语法速查表](#sql-语法速查表)

---

## 核心概念

| 概念 | SQL 关键字 | 说明 |
|------|-----------|------|
| **TABLE** | `CREATE TABLE` | 系统目录（Catalog）中的静态逻辑定义。只记录外部数据源的连接信息、格式和 Schema，不消耗任何计算资源。 |
| **STREAMING TABLE** | `CREATE STREAMING TABLE ... AS SELECT` | 持续运行的物理数据管道。引擎会在后台拉起真实的分布式计算任务，并将结果以纯追加（Append-only）方式持续写入外部系统。 |
| **事件时间 (Event Time)** | `WATERMARK FOR <column>` | 引擎内部用于推进时间进度的时间戳列。 |
| **水位线 (Watermark)** | `AS <column> - INTERVAL ...` | 对迟到乱序数据的容忍度。超过水位线的事件将被丢弃。 |

> 支持的连接器、数据格式和 SQL 数据类型的完整参考，请参阅 [连接器、格式与类型参考](connectors-and-formats-zh.md)。

---

## 第一部分：注册数据源 (TABLE)

`TABLE` 是系统目录（Catalog）中的静态逻辑定义。它只记录外部数据源（如 Kafka）的连接信息、格式和 Schema，**不消耗任何计算资源**。

在流计算中，我们必须为输入流指定**事件时间（Event Time）**和**水位线（Watermark）**，以此作为引擎内部推进时间、触发计算的唯一依据。

### 示例：注册广告曝光流与点击流

```sql
-- 1. 注册广告曝光流
CREATE TABLE ad_impressions (
    impression_id VARCHAR,
    ad_id BIGINT,
    campaign_id BIGINT,
    user_id VARCHAR,
    impression_time TIMESTAMP NOT NULL,
    -- 核心：将 impression_time 设为事件时间，并容忍最多 2 秒的数据迟到乱序
    WATERMARK FOR impression_time AS impression_time - INTERVAL '2' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_ad_impressions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- 2. 注册广告点击流
CREATE TABLE ad_clicks (
    click_id VARCHAR,
    impression_id VARCHAR,
    ad_id BIGINT,
    click_time TIMESTAMP NOT NULL,
    WATERMARK FOR click_time AS click_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_ad_clicks',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);
```

**关键要素：**

- `WATERMARK FOR <列> AS <列> - INTERVAL '<n>' SECOND`：声明事件时间列以及允许的最大乱序延迟。
- `WITH (...)`：连接器属性 — 类型、Topic、格式、Broker 地址。

---

## 第二部分：构建实时 Pipeline (STREAMING TABLE)

`STREAMING TABLE` 是持续运行的物理数据管道。使用 `CREATE STREAMING TABLE ... AS SELECT`（CTAS）语法，引擎会在后台拉起真实的分布式计算任务，并将结果以**纯追加（Append-only）**的方式持续写入外部系统。

### 场景 1：滚动窗口 (Tumbling Window)

将时间切分为互不重叠的固定窗口。

```sql
-- 需求：每 1 分钟统计一次各广告计划的曝光总量
CREATE STREAMING TABLE metric_tumble_impressions_1m WITH (
    'connector' = 'kafka',
    'topic' = 'sink_impressions_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    TUMBLE(INTERVAL '1' MINUTE) AS time_window,
    campaign_id,
    COUNT(*) AS total_impressions
FROM ad_impressions
GROUP BY
    1, -- 指代 SELECT 中的第一个字段 (time_window)
    campaign_id;
```

### 场景 2：滑动窗口 (Hopping Window)

窗口之间存在重叠，用于平滑趋势监控。

```sql
-- 需求：统计过去 10 分钟内各广告的独立访客数(UV)，每 1 分钟刷新一次
CREATE STREAMING TABLE metric_hop_uv_10m WITH (
    'connector' = 'kafka',
    'topic' = 'sink_uv_10m_step_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    HOP(INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS time_window,
    ad_id,
    COUNT(DISTINCT CAST(user_id AS STRING)) AS unique_users
FROM ad_impressions
GROUP BY
    1,
    ad_id;
```

### 场景 3：会话窗口 (Session Window)

会话窗口根据指定的不活跃间隔（Gap）对事件进行分组。如果在 Gap 时间内没有新事件到达，窗口关闭并输出结果。会话窗口非常适合用户行为会话分析。

```sql
-- 需求：按用户检测广告曝光会话，30 秒无活动则会话结束
CREATE STREAMING TABLE metric_session_impressions WITH (
    'connector' = 'kafka',
    'topic' = 'sink_session_impressions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    SESSION(INTERVAL '30' SECOND) AS time_window,
    user_id,
    COUNT(*) AS impressions_in_session
FROM ad_impressions
GROUP BY
    1,
    user_id;
```

### 场景 4：窗口双流关联 (Window Join)

将两条流在完全相同的时间窗口内进行等值关联。因为状态限定在窗口内，水位线越过窗口后状态会自动清理，绝不发生内存泄漏（OOM）。

```sql
-- 需求：精确计算 5 分钟级别的点击率 (CTR)
CREATE STREAMING TABLE metric_window_join_ctr_5m WITH (
    'connector' = 'kafka',
    'topic' = 'sink_ctr_5m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    imp.time_window,
    imp.ad_id,
    imp.impressions,
    COALESCE(clk.clicks, 0) AS clicks
FROM (
    SELECT TUMBLE(INTERVAL '5' MINUTE) AS time_window, ad_id, COUNT(*) AS impressions
    FROM ad_impressions
    GROUP BY 1, ad_id
) imp
LEFT JOIN (
    SELECT TUMBLE(INTERVAL '5' MINUTE) AS time_window, ad_id, COUNT(*) AS clicks
    FROM ad_clicks
    GROUP BY 1, ad_id
) clk
ON imp.time_window = clk.time_window AND imp.ad_id = clk.ad_id;
```

> **要求：**关联条件**必须**包含相同的时间窗口列，以确保状态有界。

---

## 第三部分：生命周期与流任务管理

Function Stream 提供了一套完整的运维指令，帮助您管理元数据目录、排查物理执行图以及销毁流计算任务。

### 1. 数据源与元数据管理

**查看所有已注册的数据源表：**

```sql
SHOW TABLES;
```

列出当前 Catalog 中的所有静态表定义及其对应的 Event Time 与 Watermark 策略。

**查看原始建表语句（DDL）：**

```sql
SHOW CREATE TABLE ad_clicks;
```

用于导出或排查某张表的底层连接参数（如 Kafka Topic、Format 等）。

### 2. 实时 Pipeline 监控与排障

**查看当前运行的计算流：**

```sql
SHOW STREAMING TABLES;
```

输出字段说明：

| 字段 | 说明 |
|------|------|
| `job_id` | 计算流的名称（如 `metric_tumble_impressions_1m`）。 |
| `status` | 当前生命周期状态（如 `RUNNING`、`FAILED`）。 |
| `pipeline_count` | 该任务在底层被拆分成的并行算子链数量。 |
| `uptime` | 任务已持续运行的时长。 |

**洞察物理执行拓扑 (Execution Graph)：**

```sql
SHOW CREATE STREAMING TABLE metric_tumble_impressions_1m;
```

这是 Function Stream 极其强大的排障指令。它会以 ASCII 格式打印出一条 SQL 是如何在底层被转化为真实分布式计算图的：

- `[Source]` — 从连接器读取数据。
- `[Operator] ExpressionWatermark` — 注入水位线。
- `[Shuffle]` — 重分布网络数据。
- `[Operator] TumblingWindowAggregate` — 执行真正的窗口聚合。
- `[Sink] ConnectorSink` — 将结果发往目标连接器（如 Kafka）。

### 3. 安全停止与释放资源

当某个实时大屏活动结束，或者您需要更新计算逻辑时，必须显式销毁旧的流任务：

```sql
DROP STREAMING TABLE metric_tumble_impressions_1m;
```

---

## SQL 语法速查表

| 语句 | 说明 |
|------|------|
| `CREATE TABLE ... WITH (...)` | 注册外部数据源，声明 Schema、事件时间和水位线。 |
| `CREATE STREAMING TABLE ... WITH (...) AS SELECT ...` | 创建并启动持续运行的流计算管道。 |
| `SHOW TABLES` | 列出所有已注册的数据源表。 |
| `SHOW CREATE TABLE <name>` | 显示某张表的建表 DDL。 |
| `SHOW STREAMING TABLES` | 列出所有正在运行的流计算管道及其状态。 |
| `SHOW CREATE STREAMING TABLE <name>` | 查看某条管道的物理执行拓扑图。 |
| `DROP STREAMING TABLE <name>` | 销毁流计算管道并释放所有资源。 |
