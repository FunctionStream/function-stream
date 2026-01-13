// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use wasmtime::component::bindgen;

// 生成 Component Model 绑定
bindgen!({
    world: "processor",
    path: "../../wit/processor.wit",
});

// 实现 Processor trait
struct ProcessorImpl;

// 实现导出的函数
impl Processor for ProcessorImpl {
    fn init(config: Vec<(String, String)>) {
        // 初始化处理器
        // config 是一个键值对列表
        println!("Processor initialized with {} config entries", config.len());
        for (key, value) in config {
            println!("  {} = {}", key, value);
        }
    }

    fn process(source_id: u32, data: Vec<u8>) {
        // 处理输入数据
        // 这里实现一个简单的 echo 处理器：将输入数据原样输出到 target-id 0
        
        let input_str = String::from_utf8_lossy(&data);
        let processed = format!("[Processed by source-{}] {}", source_id, input_str);
        
        // 通过 collector.emit 发送处理后的数据
        // 注意：Collector 是由 host 提供的导入接口，通过生成的绑定调用
        Collector::emit(0, processed.as_bytes().to_vec());
    }

    fn process_watermark(source_id: u32, watermark: u64) {
        // 处理 watermark
        println!("Received watermark {} from source {}", watermark, source_id);
        
        // 通过 collector.emit_watermark 发送 watermark
        Collector::emit_watermark(0, watermark);
    }

    fn take_checkpoint(checkpoint_id: u64) -> Vec<u8> {
        // 创建检查点
        println!("Taking checkpoint {}", checkpoint_id);
        
        // 简单的检查点数据：将 checkpoint_id 编码为字节
        checkpoint_id.to_le_bytes().to_vec()
    }

    fn check_heartbeat() -> bool {
        // 检查心跳，返回 true 表示健康
        true
    }

    fn close() {
        // 清理资源
        println!("Processor closed");
    }

    fn exec_custom(payload: Vec<u8>) -> Vec<u8> {
        // 执行自定义命令
        // 这里实现一个简单的 echo 命令
        payload
    }
}

// 导出 Processor 实现
export!(ProcessorImpl);
