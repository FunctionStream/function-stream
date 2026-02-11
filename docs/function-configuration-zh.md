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

# 任务配置

Function Stream 的任务定义采用插件化架构。虽然当前版本重点支持 Kafka 协议，但其配置模型在设计上支持通过 input-type 与 output-type 扩展至任意流媒体系统。

---

## 一、配置核心结构

| 字段           | 类型     | 必填 | 说明                              |
|--------------|--------|----|---------------------------------|
| name         | string | 是  | 任务全局唯一标识。                       |
| type         | string | 是  | 引擎类型：processor (WASM) 或 python。 |
| input-groups | array  | 是  | 逻辑输入组。支持将多个物理 Topic 聚合为一个逻辑流。   |
| outputs      | array  | 是  | 输出目标集合。支持处理结果的多路分发。             |

---

## 二、输入源配置 (input-groups)

系统通过 input-type 标识符动态加载对应的接入插件。

### 2.1 抽象接口参数

| 通用字段       | 说明                     |
|------------|------------------------|
| input-type | 驱动类型标识。目前系统内置支持 kafka。 |

### 2.2 Kafka 驱动实现 (当前支持)

当 input-type: kafka 时，需提供以下连接参数：

| 参数                | 必填 | 说明                  |
|-------------------|----|---------------------|
| bootstrap_servers | 是  | 集群访问地址。             |
| topic             | 是  | 源 Topic。            |
| group_id          | 是  | 消费者组 ID。            |
| partition         | 否  | 指定分区号，缺省时由集群自动负载均衡。 |

---

## 三、输出目标配置 (outputs)

输出端同样采用插件化设计，允许将同一份处理结果推送到不同的下游生态。

### 3.1 抽象接口参数

| 通用字段        | 说明                     |
|-------------|------------------------|
| output-type | 目标驱动标识。目前系统内置支持 kafka。 |

### 3.2 Kafka 驱动实现 (当前支持)

当 output-type: kafka 时，需提供以下参数：

| 参数                | 必填 | 说明         |
|-------------------|----|------------|
| bootstrap_servers | 是  | 目标集群地址。    |
| topic             | 是  | 目标 Topic。  |
| partition         | 是  | 显式指定写入的分区。 |
