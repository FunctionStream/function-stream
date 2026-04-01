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

# Connectors, Formats & Data Types

[中文](connectors-and-formats-zh.md) | [English](connectors-and-formats.md)

This document is the authoritative reference for connectors (sources & sinks), serialization formats, and SQL data types supported by Function Stream's Streaming SQL engine.

---

## Table of Contents

- [1. Connectors](#1-connectors)
  - [1.1 Kafka (Source)](#11-kafka-source)
  - [1.2 Kafka (Sink)](#12-kafka-sink)
- [2. Data Format](#2-data-format)
- [3. SQL Data Types](#3-sql-data-types)
- [4. Full Example](#4-full-example)

---

## 1. Connectors

Currently Function Stream supports **Kafka** as the production-ready connector for both source (ingestion) and sink (egress).

### 1.1 Kafka (Source)

A Kafka source reads records from one or more Kafka topic partitions. Use it in `CREATE TABLE` to register an input stream.

**Required Properties:**

| Property | Description | Example |
|----------|-------------|---------|
| `connector` | Must be `kafka`. | `'kafka'` |
| `topic` | Kafka topic to consume from. | `'raw_events'` |
| `format` | Serialization format of messages. | `'json'` |
| `bootstrap.servers` | Comma-separated list of Kafka broker addresses. | `'broker1:9092,broker2:9092'` |

**Example:**

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

### 1.2 Kafka (Sink)

A Kafka sink writes records into a Kafka topic. It is configured in the `WITH` clause of a `CREATE STREAMING TABLE` statement.

**Required Properties:**

| Property | Description | Example |
|----------|-------------|---------|
| `connector` | Must be `kafka`. | `'kafka'` |
| `topic` | Kafka topic to write to. | `'sink_results'` |
| `format` | Serialization format of output messages. | `'json'` |
| `bootstrap.servers` | Comma-separated Kafka broker addresses. | `'broker1:9092'` |

**Example:**

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

## 2. Data Format

Currently the only supported serialization format is **JSON**. Each Kafka message is expected to be a self-describing JSON object whose fields map directly to the columns defined in `CREATE TABLE`.

Set `'format' = 'json'` in the `WITH` clause (this is also the default when omitted).

---

## 3. SQL Data Types

The following SQL data types are supported in `CREATE TABLE` column definitions:

### Numeric Types

| SQL Type | Aliases | Arrow Type | Description |
|----------|---------|------------|-------------|
| `BOOLEAN` | `BOOL` | Boolean | True / false. |
| `TINYINT` | — | Int8 | 8-bit signed integer. |
| `SMALLINT` | `INT2` | Int16 | 16-bit signed integer. |
| `INT` | `INTEGER`, `INT4` | Int32 | 32-bit signed integer. |
| `BIGINT` | `INT8` | Int64 | 64-bit signed integer. |
| `TINYINT UNSIGNED` | — | UInt8 | 8-bit unsigned integer. |
| `SMALLINT UNSIGNED` | `INT2 UNSIGNED` | UInt16 | 16-bit unsigned integer. |
| `INT UNSIGNED` | `INT4 UNSIGNED` | UInt32 | 32-bit unsigned integer. |
| `BIGINT UNSIGNED` | `INT8 UNSIGNED` | UInt64 | 64-bit unsigned integer. |
| `FLOAT` | `REAL`, `FLOAT4` | Float32 | 32-bit IEEE 754 floating point. |
| `DOUBLE` | `DOUBLE PRECISION`, `FLOAT8` | Float64 | 64-bit IEEE 754 floating point. |
| `DECIMAL(p, s)` | `NUMERIC(p, s)` | Decimal128 | Fixed-point decimal. Precision 1–38, scale <= precision. |

### String & Binary Types

| SQL Type | Aliases | Arrow Type | Description |
|----------|---------|------------|-------------|
| `VARCHAR` | `TEXT`, `STRING`, `CHAR` | Utf8 | Variable-length UTF-8 string. |
| `BYTEA` | — | Binary | Variable-length byte array. |
| `JSON` | — | Utf8 (JSON extension) | JSON-typed string with FunctionStream extension metadata. |

### Date & Time Types

| SQL Type | Arrow Type | Description |
|----------|------------|-------------|
| `TIMESTAMP` | Timestamp(Nanosecond) | Date and time without timezone (nanosecond precision). |
| `TIMESTAMP(0)` | Timestamp(Second) | Second precision. |
| `TIMESTAMP(3)` | Timestamp(Millisecond) | Millisecond precision. |
| `TIMESTAMP(6)` | Timestamp(Microsecond) | Microsecond precision. |
| `TIMESTAMP(9)` | Timestamp(Nanosecond) | Nanosecond precision (same as `TIMESTAMP`). |
| `DATE` | Date32 | Calendar date (year, month, day). |
| `DATETIME` | Timestamp(Nanosecond) | Alias for `TIMESTAMP`. |
| `TIME` | Time64(Nanosecond) | Time of day without timezone. |
| `INTERVAL` | Interval(MonthDayNano) | Time duration / interval. |

### Composite Types

| SQL Type | Arrow Type | Description |
|----------|------------|-------------|
| `STRUCT<name type, ...>` | Struct | Named composite fields. |
| `ARRAY<element_type>` | List | Ordered list of elements of the same type. Also supports `element_type[]` syntax. |

---

## 4. Full Example

Below is a complete example combining a Kafka source, a Kafka sink, JSON format, and various SQL data types:

```sql
-- Source: user activity events from Kafka
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

-- Sink: 1-minute tumbling window aggregation
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
