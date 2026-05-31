# ROS2 Source（rosbridge）

`ros2` source 通过 **rosbridge_suite** WebSocket 订阅 ROS2 topic，协议与 `ros` source 相同，默认按 ROS2 版本注册。

## 前置条件

ROS2 环境安装并启动 rosbridge：

```bash
ros2 launch rosbridge_server rosbridge_websocket_launch.xml
```

默认 WebSocket：`ws://127.0.0.1:9090`

## 支持格式

- `json`（推荐）
- `raw_string` / `raw_bytes`

## 常用 WITH 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `connector='ros2'` | 固定 | — |
| `topic` | ROS2 topic，如 `/joint_states` | 必填 |
| `url` / `ros.url` | rosbridge 地址 | `ws://127.0.0.1:9090` |
| `ros.message.field` | publish 帧中的 JSON 字段 | `msg` |
| `format` | 如 `json` | 必填 |

## 示例

```sql
CREATE TABLE ros2_joints (
  name STRING,
  position STRING,
  ts TIMESTAMP
) WITH (
  connector='ros2',
  type='source',
  topic='/joint_states',
  url='ws://127.0.0.1:9090',
  format='json'
);
```

## ROS1 与 ROS2 区别

| 项 | `ros` | `ros2` |
|----|-------|--------|
| 默认 `ros.version` | ros1 | ros2 |
| 桥接服务 | rosbridge (ROS1) | rosbridge_server (ROS2) |
| WebSocket 协议 | 相同 | 相同 |

也可在 `connector='ros'` 时显式设置 `'ros.version'='ros2'`。

## 并行度

建议 **`parallelism = 1`**。
