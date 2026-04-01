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

# Streaming SQL Guide

[中文](streaming-sql-guide-zh.md) | [English](streaming-sql-guide.md)

Function Stream provides a declarative SQL interface for building real-time stream processing pipelines. With Streaming SQL you can ingest unbounded data streams, perform time-windowed aggregations, join multiple streams, and manage pipeline lifecycles — all without writing imperative code.

---

## Table of Contents

- [Core Concepts](#core-concepts)
- [Part 1: Registering Data Sources (TABLE)](#part-1-registering-data-sources-table)
- [Part 2: Building Real-Time Pipelines (STREAMING TABLE)](#part-2-building-real-time-pipelines-streaming-table)
  - [Tumbling Window](#scenario-1-tumbling-window)
  - [Hopping Window](#scenario-2-hopping-window)
  - [Window Join](#scenario-3-window-join)
- [Part 3: Lifecycle & Pipeline Management](#part-3-lifecycle--pipeline-management)
  - [Data Source Management](#1-data-source--metadata-management)
  - [Pipeline Monitoring](#2-real-time-pipeline-monitoring--troubleshooting)
  - [Stopping & Cleanup](#3-safe-shutdown--resource-release)
- [SQL Reference Summary](#sql-reference-summary)

---

## Core Concepts

| Concept | SQL Keyword | Description |
|---------|-------------|-------------|
| **TABLE** | `CREATE TABLE` | A static logical definition in the catalog. Records external source connection info, format, and schema. Consumes no compute resources. |
| **STREAMING TABLE** | `CREATE STREAMING TABLE ... AS SELECT` | A physically running data pipeline. The engine allocates distributed compute tasks and continuously writes results to external systems in append-only mode. |
| **Event Time** | `WATERMARK FOR <column>` | The timestamp column used by the engine to track the progression of time within a stream. |
| **Watermark** | `AS <column> - INTERVAL ...` | A tolerance for late-arriving, out-of-order data. Events arriving after the watermark are dropped. |

> For the full reference on supported connectors, data formats, and SQL data types, see [Connectors, Formats & Data Types](connectors-and-formats.md).

---

## Part 1: Registering Data Sources (TABLE)

A `TABLE` is a static logical definition in the system catalog. It only records the connection information (e.g. Kafka broker, topic), data format, and schema of an external data source. **It does not consume any compute resources.**

In stream processing, you must specify an **Event Time** column and a **Watermark** strategy for each input stream. The engine uses these as the sole basis for advancing time and triggering computations.

### Example: Register an Ad-Impressions Stream and a Clicks Stream

```sql
-- 1. Register the ad-impressions stream
CREATE TABLE ad_impressions (
    impression_id VARCHAR,
    ad_id BIGINT,
    campaign_id BIGINT,
    user_id VARCHAR,
    impression_time TIMESTAMP NOT NULL,
    WATERMARK FOR impression_time AS impression_time - INTERVAL '2' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'raw_ad_impressions',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
);

-- 2. Register the ad-clicks stream
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

**Key elements:**

- `WATERMARK FOR <column> AS <column> - INTERVAL '<n>' SECOND`: declares the event-time column and the maximum tolerated out-of-order delay.
- `WITH (...)`: connector properties — type, topic, format, and broker address.

---

## Part 2: Building Real-Time Pipelines (STREAMING TABLE)

A `STREAMING TABLE` is a continuously running physical data pipeline. Using the `CREATE STREAMING TABLE ... AS SELECT` (CTAS) syntax, the engine launches real distributed compute tasks in the background and continuously writes results to an external system in **append-only** mode.

### Scenario 1: Tumbling Window

Divides time into fixed, non-overlapping windows.

```sql
-- Count total impressions per campaign every 1 minute
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
    1,
    campaign_id;
```

### Scenario 2: Hopping Window

Windows overlap, useful for smoothed trend monitoring.

```sql
-- Count distinct visitors (UV) per ad over the last 10 minutes, refreshed every 1 minute
CREATE STREAMING TABLE metric_hop_uv_10m WITH (
    'connector' = 'kafka',
    'topic' = 'sink_uv_10m_step_1m',
    'format' = 'json',
    'bootstrap.servers' = 'localhost:9092'
) AS
SELECT
    HOP(INTERVAL '1' MINUTE, INTERVAL '10' MINUTE) AS time_window,
    ad_id,
    COUNT(DISTINCT user_id) AS unique_users
FROM ad_impressions
GROUP BY
    1,
    ad_id;
```

### Scenario 3: Window Join

Join two streams within exactly the same time window. Because state is bounded by the window, memory is automatically reclaimed once the watermark advances past the window boundary — eliminating the risk of OOM.

```sql
-- Calculate 5-minute click-through rate (CTR)
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

> **Requirement:** The join condition **must** include the same time-window column to ensure bounded state.

---

## Part 3: Lifecycle & Pipeline Management

Function Stream provides a complete set of operational commands for managing the metadata catalog, inspecting physical execution graphs, and destroying streaming pipelines.

### 1. Data Source & Metadata Management

**List all registered source tables:**

```sql
SHOW TABLES;
```

Lists all static table definitions in the current catalog along with their Event Time and Watermark strategies.

**Show the original DDL of a table:**

```sql
SHOW CREATE TABLE ad_clicks;
```

Useful for exporting or auditing the underlying connection parameters (Kafka topic, format, etc.).

### 2. Real-Time Pipeline Monitoring & Troubleshooting

**List all running streaming pipelines:**

```sql
SHOW STREAMING TABLES;
```

Output columns:

| Column | Description |
|--------|-------------|
| `job_id` | Pipeline name (e.g. `metric_tumble_impressions_1m`). |
| `status` | Lifecycle state (`RUNNING`, `FAILED`, etc.). |
| `pipeline_count` | Number of parallel operator chains the engine split the job into. |
| `uptime` | How long the pipeline has been running. |

**Inspect the physical execution topology:**

```sql
SHOW CREATE STREAMING TABLE metric_tumble_impressions_1m;
```

This prints an ASCII representation of how the SQL was translated into a distributed execution graph:

- `[Source]` — reads from the connector.
- `[Operator] ExpressionWatermark` — injects watermarks.
- `[Shuffle]` — redistributes data across the network.
- `[Operator] TumblingWindowAggregate` — performs the actual windowed aggregation.
- `[Sink] ConnectorSink` — writes results to the target connector (e.g. Kafka).

### 3. Safe Shutdown & Resource Release

When a campaign ends or you need to update the pipeline logic, explicitly destroy the old streaming pipeline:

```sql
DROP STREAMING TABLE metric_tumble_impressions_1m;
```

---

## SQL Reference Summary

| Statement | Description |
|-----------|-------------|
| `CREATE TABLE ... WITH (...)` | Register an external data source with schema, event time, and watermark. |
| `CREATE STREAMING TABLE ... WITH (...) AS SELECT ...` | Create and launch a continuous streaming pipeline. |
| `SHOW TABLES` | List all registered source tables. |
| `SHOW CREATE TABLE <name>` | Display the DDL of a registered table. |
| `SHOW STREAMING TABLES` | List all running streaming pipelines with status. |
| `SHOW CREATE STREAMING TABLE <name>` | Inspect the physical execution graph of a pipeline. |
| `DROP STREAMING TABLE <name>` | Destroy a streaming pipeline and release all resources. |
