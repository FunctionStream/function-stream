//go:build wasi || wasm

package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"

	"go.bytecodealliance.org/cm"

	"github.com/function-stream/go-processor-example/bindings/functionstream/core/collector"
	"github.com/function-stream/go-processor-example/bindings/functionstream/core/kv"
	"github.com/function-stream/go-processor-example/bindings/functionstream/core/processor"
)

// 全局状态存储
var store kv.Store

// 计数器映射（用于批量处理）
var counterMap map[string]int64

// 初始化状态
func init() {
	counterMap = make(map[string]int64)

	// 注册导出函数到 processor.Exports（函数名已加 fs 前缀）
	processor.Exports.FsInit = FsInit
	processor.Exports.FsProcess = FsProcess
	processor.Exports.FsProcessWatermark = FsProcessWatermark
	processor.Exports.FsTakeCheckpoint = FsTakeCheckpoint
	processor.Exports.FsCheckHeartbeat = FsCheckHeartbeat
	processor.Exports.FsClose = FsClose
	processor.Exports.FsExecCustom = FsExecCustom
}

// FsInit 初始化处理器
// WIT: export fs-init: func(config: list<tuple<string, string>>);
func FsInit(config cm.List[[2]string]) {
	// 初始化处理器
	// config 是一个键值对列表
	configSlice := config.Slice()
	fmt.Printf("Processor initialized with %d config entries\n", len(configSlice))
	for _, entry := range configSlice {
		fmt.Printf("  %s = %s\n", entry[0], entry[1])
	}

	// 打开状态存储（使用 constructor）
	// constructor 不能返回 Result，如果出错会抛出异常
	store = kv.NewStore("counter-store")
	fmt.Println("State store opened successfully")
}

// FsProcess 处理输入数据
// WIT: export fs-process: func(source-id: u32, data: list<u8>);
func FsProcess(sourceID uint32, data cm.List[uint8]) {
	// 将 cm.List[uint8] 转换为 []byte
	dataBytes := data.Slice()
	inputStr := string(dataBytes)

	// 从状态存储中读取当前计数
	key := cm.ToList([]byte(inputStr))
	result := store.GetState(key)

	var count int64 = 0
	if result.IsOK() {
		opt := result.OK()
		if opt != nil && !opt.None() {
			valueList := opt.Some()
			if valueList != nil {
				valueBytes := valueList.Slice()
				// 将字节数组转换为 int64
				if len(valueBytes) == 8 {
					count = int64(binary.LittleEndian.Uint64(valueBytes))
				}
			}
		}
	}

	// 增加计数
	count++

	// 更新内存中的计数器（用于批量处理和输出）
	counterMap[inputStr] = count

	// 将计数保存到状态存储
	countBytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(countBytes, uint64(count))
	putResult := store.PutState(key, cm.ToList(countBytes))
	if putResult.IsErr() {
		err := putResult.Err()
		fmt.Printf("Failed to put state for key %s: %v\n", inputStr, err)
		return
	}

	// 每 5 个为一组，输出 JSON Map
	// 当某个 key 的计数达到 5 的倍数时，输出当前所有计数器的状态
	if count%5 == 0 {
		// 构建 JSON Map（包含所有当前计数）
		resultMap := make(map[string]int64)
		for k, v := range counterMap {
			resultMap[k] = v
		}

		// 序列化为 JSON
		jsonBytes, err := json.Marshal(resultMap)
		if err != nil {
			fmt.Printf("Failed to marshal JSON: %v\n", err)
			return
		}

		// 发送处理后的数据
		fmt.Printf("Emitting result for key %s (count: %d): %s\n", inputStr, count, string(jsonBytes))
		collector.Emit(0, cm.ToList(jsonBytes))
	}
}

// FsProcessWatermark 处理 watermark
// WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
func FsProcessWatermark(sourceID uint32, watermark uint64) {
	// 处理 watermark
	fmt.Printf("Received watermark %d from source %d\n", watermark, sourceID)

	// 通过 collector.emit_watermark 发送 watermark
	collector.EmitWatermark(0, watermark)
}

// FsTakeCheckpoint 创建检查点
// WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
func FsTakeCheckpoint(checkpointID uint64) cm.List[uint8] {
	// 创建检查点
	// 将当前计数器映射序列化为 JSON
	fmt.Printf("Taking checkpoint %d\n", checkpointID)

	// 将计数器映射序列化为 JSON
	jsonBytes, err := json.Marshal(counterMap)
	if err != nil {
		fmt.Printf("Failed to marshal checkpoint data: %v\n", err)
		// 返回空的检查点数据
		return cm.ToList([]byte{})
	}

	return cm.ToList(jsonBytes)
}

// FsCheckHeartbeat 检查心跳
// WIT: export fs-check-heartbeat: func() -> bool;
func FsCheckHeartbeat() bool {
	// 检查心跳，返回 true 表示健康
	return true
}

// FsClose 清理资源
// WIT: export fs-close: func();
func FsClose() {
	// 清理资源
	fmt.Println("Processor closed")

	// 关闭状态存储
	if store != 0 {
		store.ResourceDrop()
	}

	// 清空计数器映射
	counterMap = nil
}

// FsExecCustom 执行自定义命令
// WIT: export fs-exec-custom: func(payload: list<u8>) -> list<u8>;
func FsExecCustom(payload cm.List[uint8]) cm.List[uint8] {
	// 执行自定义命令
	// 这里实现一个简单的 echo 命令
	return payload
}

// main 函数在 WASM 中不会被调用，但需要存在
func main() {
	// WASM 入口点
}

// cabi_realloc 是 Component Model 所需的导出函数
// TinyGo 不会自动生成这个函数，需要手动添加
//
//go:export cabi_realloc
func cabi_realloc(ptr uintptr, old_size, align, new_size uint32) uintptr {
	// 这是一个占位符实现，实际的内存管理由 TinyGo 运行时处理
	// 返回 0 表示分配失败，实际应该调用内存分配函数
	// 但为了通过 wasm-tools 的检查，这里提供一个最小实现
	return 0
}
