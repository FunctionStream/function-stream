# ROS Source（rosbridge / ROS1）

`ros` source 通过 [rosbridge_suite](https://github.com/RobotWebTools/rosbridge_suite) WebSocket 订阅 ROS topic。

ROS2 请使用 [`ros2`](ros2-source-zh.md) connector 或设置 `'ros.version'='ros2'`。

## 支持格式

- `json`
- `raw_string` / `raw_bytes`

## 常用 WITH 参数

| 参数 | 说明 | 默认 |
|------|------|------|
| `connector='ros'` | 固定 | — |
| `topic` | ROS topic | 必填 |
| `url` | rosbridge WebSocket | `ws://127.0.0.1:9090` |
| `ros.message.field` | JSON 载荷字段 | `msg` |
| `format` | 如 `json` | 必填 |

## 示例

```sql
CREATE TABLE ros1_chatter (
  data STRING,
  ts TIMESTAMP
) WITH (
  connector='ros',
  type='source',
  topic='/chatter',
  url='ws://127.0.0.1:9090',
  format='json'
);
```

## 相关文档

- [ROS2 Source](ros2-source-zh.md)
- [Robot Bag 文件 Source](robot-bag-source-zh.md)
- [机器人 Source 总览](robot-sources-zh.md)
