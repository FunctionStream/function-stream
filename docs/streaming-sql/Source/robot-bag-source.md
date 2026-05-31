# Robot Bag Source

The `robot-bag` source replays offline robot recordings from industrial file formats.

## Supported file formats

| `bag.format` | File type | Description |
|--------------|-----------|-------------|
| `auto` | Extension / magic bytes | Default |
| `mcap` | `.mcap` | Foxglove / MCAP recordings |
| `ros1` | `.bag` | ROS1 rosbag (v1.02 / v2.0, chunk + index) |
| `ros2` | `.db3` or directory | ROS2 rosbag2 (SQLite) |
| `pcd` | `.pcd` or directory | PCL PCD v0.7 (`ascii` / `binary` / `binary_compressed`) |
| `jsonl` | `.jsonl` / `.ndjson` | One JSON object per line |
| `csv` | `.csv` | Header row skipped, one record per line |

Payload decoding is controlled by `format` (typically `json` or `raw_bytes`).

## Common `WITH` options

| Option | Description | Default |
|--------|-------------|---------|
| `connector='robot-bag'` | Required | — |
| `path` | File or directory | Required |
| `bag.format` | See table above | `auto` |
| `topic` | Filter by topic (MCAP / ROS1 / ROS2) | All topics |
| `pcd.emit.mode` | PCD: `point` (one row per point) or `cloud` (one row per file) | `point` |
| `replay.interval.ms` | Delay between messages (`0` = as fast as possible) | `0` |
| `loop` | Reload file after EOF | `false` |
| `format` | Payload format (`json` / `raw_string` / `raw_bytes`) | Required |

## Auto-detection

When `bag.format='auto'`:

- **Directory** with `.pcd` files → PCD
- **Directory** without `.pcd` → ROS2 bag
- **File** starting with `#ROSBAG V2.0` / `#ROSBAG V1.02` → ROS1
- **Extension**: `.mcap`, `.bag`, `.db3`, `.pcd`, `.jsonl`, `.csv`

## Example: ROS1 bag

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

ROS1 bags are parsed via chunk/index records and replayed in timestamp order. Use `format='raw_bytes'` for binary messages such as `sensor_msgs/PointCloud2`.

## Example: PCD point cloud (per point)

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

Each point is emitted as a JSON object with field names from the PCD header (`x`, `y`, `z`, `intensity`, etc.) plus `_source_file` and `_point_index`. PCL v0.7 `binary_compressed` uses LZF decompression and SoA→AoS reordering. In directory mode, all `.pcd` files are loaded in sorted filename order.

## Example: PCD whole cloud (single JSON row)

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

## Example: MCAP

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

## Example: ROS2 bag directory

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

## Example: JSONL log with loop replay

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

## End of replay

When `loop=false`, the source emits `EndOfStream` after the last message (buffered batches are flushed first). When `loop=true`, the file is reloaded and replay starts again.

## Parallelism

Use **`parallelism = 1`**. File replay is single-threaded and not partitionable.
