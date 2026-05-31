# Robot Bag Source（机器人录制文件）

`robot-bag` source 用于回放机器人离线数据，支持多种工业常见文件格式。

## 支持格式

| `bag.format` | 文件类型 | 说明 |
|--------------|----------|------|
| `auto` | 按扩展名/魔数推断 | 默认 |
| `mcap` | `.mcap` | Foxglove / MCAP 录制 |
| `ros1` | `.bag` | ROS1 rosbag（v1.02 / v2.0，含 chunk/index） |
| `ros2` | `.db3` 或目录 | ROS2 rosbag2（SQLite） |
| `pcd` | `.pcd` 或目录 | PCL PCD v0.7（ascii / binary / binary_compressed） |
| `jsonl` | `.jsonl` / `.ndjson` | 每行一条 JSON |
| `csv` | `.csv` | 跳过表头，每行一条记录 |

消息体解析格式由 `format` 指定（通常 `json` 或 `raw_bytes`）。

## 常用 WITH 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `connector='robot-bag'` | 固定 | — |
| `path` | 文件或目录 | 必填 |
| `bag.format` | 见上表 | `auto` |
| `topic` | MCAP/ROS1/ROS2 按 topic 过滤 | 全部 |
| `pcd.emit.mode` | PCD：`point`（每点一条）或 `cloud`（每文件一条） | `point` |
| `replay.interval.ms` | 两条消息间隔（0=尽快） | `0` |
| `loop` | 播完后是否循环 | `false` |
| `format` | 消息解码格式 | 必填 |

## 自动识别（`bag.format='auto'`）

- **目录**含 `.pcd` → PCD
- **目录**无 `.pcd` → ROS2 bag
- **文件**头为 `#ROSBAG V2.0` / `#ROSBAG V1.02` → ROS1
- **扩展名**：`.mcap`、`.bag`、`.db3`、`.pcd`、`.jsonl`、`.csv`

## 示例：ROS1 bag

```sql
CREATE TABLE robot_ros1_bag (
  payload BYTES,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/lidar_run.bag',
  'bag.format'='ros1',
  topic='/velodyne_points',
  format='raw_bytes'
);
```

ROS1 bag 通过标准 `#ROSBAG V2.0` 头解析 chunk/index，按时间戳排序回放。`sensor_msgs/PointCloud2` 等二进制消息请使用 `format='raw_bytes'`。

## 示例：PCD 点云（逐点）

```sql
CREATE TABLE lidar_points (
  x DOUBLE,
  y DOUBLE,
  z DOUBLE,
  intensity DOUBLE,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/scans/',
  'bag.format'='pcd',
  'pcd.emit.mode'='point',
  format='json'
);
```

每个点输出一条 JSON，字段名来自 PCD header（`x`、`y`、`z`、`intensity` 等），并附带 `_source_file`、`_point_index`。PCD 解析遵循 PCL v0.7，支持 `ascii`、`binary`、`binary_compressed`（LZF + SoA→AoS 重排）。目录模式下按文件名排序依次加载所有 `.pcd`。

## 示例：PCD 整云（单条 JSON）

```sql
CREATE TABLE lidar_cloud (
  payload STRING,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/scan.pcd',
  'bag.format'='pcd',
  'pcd.emit.mode'='cloud',
  format='json'
);
```

## 示例：MCAP

```sql
CREATE TABLE robot_mcap (
  robot_id STRING,
  pose STRING,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/robot_run.mcap',
  'bag.format'='mcap',
  topic='/odom',
  format='json'
);
```

## 示例：ROS2 bag 目录

```sql
CREATE TABLE robot_ros2_bag (
  joint_name STRING,
  position DOUBLE,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/data/rosbag2_2024',
  'bag.format'='ros2',
  topic='/joint_states',
  format='raw_bytes'
);
```

## 示例：JSONL 循环回放

```sql
CREATE TABLE robot_jsonl (
  event STRING,
  payload STRING,
  ts TIMESTAMP
) WITH (
  connector='robot-bag',
  type='source',
  path='/logs/robot_events.jsonl',
  'bag.format'='jsonl',
  format='json',
  loop='true'
);
```

## 回放完成后

`loop=false` 时读完文件先 flush 缓冲区，再发送 `EndOfStream`；`loop=true` 时重新加载文件循环播放。可用 `replay.interval.ms` 模拟实时播放间隔。

## 并行度

建议 **`parallelism = 1`**。
