//go:build wasi || wasm

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

package main

import (
	"encoding/json"
	"strconv"
	"strings"

	"go.bytecodealliance.org/cm"

	"github.com/function-stream/go-processor-example/bindings/functionstream/core/collector"
	"github.com/function-stream/go-processor-example/bindings/functionstream/core/kv"
	"github.com/function-stream/go-processor-example/bindings/functionstream/core/processor"
)

var store kv.Store
var counterMap map[string]int64
var totalProcessed int64
var keyPrefix string

func init() {
	counterMap = make(map[string]int64)
	totalProcessed = 0
	keyPrefix = ""

	processor.Exports.FsInit = FsInit
	processor.Exports.FsProcess = FsProcess
	processor.Exports.FsProcessWatermark = FsProcessWatermark
	processor.Exports.FsTakeCheckpoint = FsTakeCheckpoint
	processor.Exports.FsCheckHeartbeat = FsCheckHeartbeat
	processor.Exports.FsClose = FsClose
	processor.Exports.FsExec = FsExec
	processor.Exports.FsCustom = FsCustom
}

// WIT: export fs-init: func(config: list<tuple<string, string>>);
func FsInit(config cm.List[[2]string]) {
	counterMap = make(map[string]int64)
	totalProcessed = 0
	keyPrefix = ""

	configSlice := config.Slice()
	for _, entry := range configSlice {
		if entry[0] == "key_prefix" {
			keyPrefix = entry[1]
		}
	}

	store = kv.NewStore("counter-store")
}

// WIT: export fs-process: func(source-id: u32, data: list<u8>);
func FsProcess(sourceID uint32, data cm.List[uint8]) {
	dataBytes := data.Slice()
	inputStr := strings.TrimSpace(string(dataBytes))
	if inputStr == "" {
		return
	}

	totalProcessed++

	fullKey := keyPrefix + inputStr
	storeKeyBytes := []byte(fullKey)
	key := cm.ToList(storeKeyBytes)
	result := store.GetState(key)

	currentCount := int64(0)
	if result.IsOK() {
		opt := result.OK()
		if opt != nil && !opt.None() {
			valueList := opt.Some()
			if valueList != nil {
				valueBytes := valueList.Slice()
				if count, err := strconv.ParseInt(string(valueBytes), 10, 64); err == nil {
					currentCount = count
				}
			}
		}
	}

	newCount := currentCount + 1
	counterMap[inputStr] = newCount

	newCountStr := strconv.FormatInt(newCount, 10)
	putResult := store.PutState(key, cm.ToList([]byte(newCountStr)))
	if putResult.IsErr() {
		return
	}

	outputPayload := map[string]interface{}{
		"total_processed": totalProcessed,
		"counter_map":     counterMap,
	}

	jsonBytes, err := json.Marshal(outputPayload)
	if err != nil {
		return
	}

	collector.Emit(0, cm.ToList(jsonBytes))
}

// WIT: export fs-process-watermark: func(source-id: u32, watermark: u64);
func FsProcessWatermark(sourceID uint32, watermark uint64) {
	collector.EmitWatermark(0, watermark)
}

// WIT: export fs-take-checkpoint: func(checkpoint-id: u64) -> list<u8>;
func FsTakeCheckpoint(checkpointID uint64) cm.List[uint8] {
	return cm.ToList([]byte{})
}

// WIT: export fs-check-heartbeat: func() -> bool;
func FsCheckHeartbeat() bool {
	return true
}

// WIT: export fs-close: func();
func FsClose() {
	counterMap = make(map[string]int64)
	totalProcessed = 0

	if store != 0 {
		store.ResourceDrop()
	}
}

// WIT: export fs-exec: func(class-name: string, modules: list<tuple<module-name: string, module-bytes: list<u8>>>);
// Note: The actual type will be determined by the WIT binding generator.
// For now, using a placeholder that matches the WIT signature.
func FsExec(className string, modules cm.List[[2]interface{}]) {
	// modules[0] should be string (module-name)
	// modules[1] should be cm.List[uint8] (module-bytes)
}

// WIT: export fs-custom: func(payload: list<u8>) -> list<u8>;
func FsCustom(payload cm.List[uint8]) cm.List[uint8] {
	return payload
}

func main() {
}

//go:export cabi_realloc
func cabi_realloc(ptr uintptr, old_size, align, new_size uint32) uintptr {
	return 0
}
