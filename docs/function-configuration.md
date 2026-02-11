# Function Configuration

Function Stream's task definition adopts a plugin-based architecture. Although the current version focuses on supporting the Kafka protocol, its configuration model is designed to support extension to any streaming media system via input-type and output-type.

---

## 1. Core Configuration Structure

| Field        | Type   | Required | Description                                                                                |
|--------------|--------|----------|--------------------------------------------------------------------------------------------|
| name         | string | Yes      | Global unique identifier for the task.                                                     |
| type         | string | Yes      | Engine type: processor (WASM) or python.                                                   |
| input-groups | array  | Yes      | Logical input groups. Supports aggregating multiple physical Topics into a logical stream. |
| outputs      | array  | Yes      | Output destination collection. Supports multi-path distribution of processing results.     |

---

## 2. Input Source Configuration (input-groups)

The system dynamically loads the corresponding access plugin via the input-type identifier.

### 2.1 Abstract Interface Parameters

| Common Field | Description                                                   |
|--------------|---------------------------------------------------------------|
| input-type   | Driver type identifier. Currently built-in support for kafka. |

### 2.2 Kafka Driver Implementation (Currently Supported)

When input-type: kafka, the following connection parameters must be provided:

| Parameter         | Required | Description                                                                    |
|-------------------|----------|--------------------------------------------------------------------------------|
| bootstrap_servers | Yes      | Cluster access address.                                                        |
| topic             | Yes      | Source Topic.                                                                  |
| group_id          | Yes      | Consumer Group ID.                                                             |
| partition         | No       | Specify partition number; defaults to automatic load balancing by the cluster. |

---

## 3. Output Destination Configuration (outputs)

The output end also adopts a plugin-based design, allowing the same processing result to be pushed to different downstream ecosystems.

### 3.1 Abstract Interface Parameters

| Common Field | Description                                                     |
|--------------|-----------------------------------------------------------------|
| output-type  | Target driver identifier. Currently built-in support for kafka. |

### 3.2 Kafka Driver Implementation (Currently Supported)

When output-type: kafka, the following parameters must be provided:

| Parameter         | Required | Description                                                                                         |
|-------------------|----------|-----------------------------------------------------------------------------------------------------|
| bootstrap_servers | Yes      | Target cluster address.                                                                             |
| topic             | Yes      | Target Topic.                                                                                       |
| partition         | Yes      | Explicitly specify the partition to write to (required by stream processing consistency semantics). |
