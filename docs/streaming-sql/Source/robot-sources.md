# Robot & Industrial Data Sources

Guide for mobile robots, manipulators, AGV, and LiDAR pipelines. Connector options follow a Flink-style `WITH` clause (`url`, `format`, `scan.interval`, etc.).

## Supported sources

| Connector | Typical use | Doc |
|-----------|-------------|-----|
| **kafka** | Aggregated topics via ROS2 bridge / platform bus | [kafka-source.md](kafka-source.md) |
| **mqtt** | Device MQTT, edge broker, IoT rules | [mqtt-source-zh.md](mqtt-source-zh.md) |
| **http** | REST poll or Webhook telemetry | [http-source-zh.md](http-source-zh.md) |
| **ros** | ROS1 topics via rosbridge WebSocket | [ros-source-zh.md](ros-source-zh.md) |
| **ros2** | ROS2 topics via rosbridge_server | [ros2-source-zh.md](ros2-source-zh.md) |
| **robot-bag** | Offline MCAP / ROS1 bag / ROS2 bag / PCD / JSONL / CSV | [robot-bag-source.md](robot-bag-source.md) |

## Selection guide

```text
Robot ──MQTT──► Broker ──► function-stream (mqtt source)
Robot ──HTTP POST──► function-stream (http webhook)
Scheduler ──HTTP GET API──► function-stream (http poll)
ROS/ROS2 ──rosbridge──► function-stream (ros / ros2 source)
Offline recording ──bag/MCAP/PCD──► function-stream (robot-bag source)
```

| Data shape | Connector | Notes |
|------------|-----------|-------|
| High-frequency telemetry, weak network | `mqtt` | QoS 1/2, stable `client_id` |
| Cloud REST callback | `http` webhook | `http.listen.port` + `http.webhook.path` |
| Periodic robot status poll | `http` poll | `url` + `scan.interval` |
| Live ROS/ROS2 topic | `ros` / `ros2` | rosbridge WebSocket |
| Offline bag / MCAP / PCD / logs | `robot-bag` | `path` + `bag.format` |
| Existing Kafka pipeline | `kafka` | `group.id`, `scan.startup.mode` |

## robot-bag format matrix

| `bag.format` | Input | Topic filter | Typical `format` |
|--------------|-------|--------------|------------------|
| `ros1` | `.bag` file | Yes | `raw_bytes` for CDR/binary ROS messages |
| `ros2` | `.db3` or directory | Yes | `raw_bytes` |
| `mcap` | `.mcap` file | Yes | `json` or `raw_bytes` |
| `pcd` | `.pcd` or directory | No | `json` (`pcd.emit.mode=point` or `cloud`) |
| `jsonl` | `.jsonl` | No | `json` |
| `csv` | `.csv` | No | `json` or `raw_string` |

## Conventions

1. Prefer **`format='json'`** for structured telemetry; use **`raw_bytes`** for ROS binary payloads.
2. Set **`parallelism = 1`** for MQTT, HTTP webhook, HTTP poll, rosbridge, and robot-bag sources.
3. Kafka source has **no rate limit**; default batch=4096, linger=20ms.
4. robot-bag replays files once unless `loop=true`; use `replay.interval.ms` to simulate real-time playback.

## End-to-end example (robot-bag PCD → Delta)

```sql
CREATE TABLE lidar_scan (
  x DOUBLE,
  y DOUBLE,
  z DOUBLE,
  intensity DOUBLE,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/lidar_frames/',
  'bag.format'='pcd',
  'pcd.emit.mode'='point',
  format='json'
);

CREATE STREAMING TABLE lidar_delta
WITH (
  connector='delta',
  type='sink',
  path='/data/lidar_out',
  format='parquet'
) AS
SELECT x, y, z, intensity, ts FROM lidar_scan;
```
