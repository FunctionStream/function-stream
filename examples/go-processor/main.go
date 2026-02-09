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
	"wit_component/bindings/functionstream/core/collector"
	"wit_component/bindings/functionstream/core/kv"
	"wit_component/bindings/functionstream/core/processor"
)

var (
	store          kv.Store
	storeInit      bool
	counterMap     map[string]int64
	totalProcessed int64
	keyPrefix      string
)

func init() {
	processor.Exports.FsInit = fsInit
	processor.Exports.FsProcess = fsProcess
	processor.Exports.FsProcessWatermark = fsProcessWatermark
	processor.Exports.FsTakeCheckpoint = fsTakeCheckpoint
	processor.Exports.FsCheckHeartbeat = fsCheckHeartbeat
	processor.Exports.FsClose = fsClose
	processor.Exports.FsExec = fsExec
	processor.Exports.FsCustom = fsCustom
}

func fsInit(config cm.List[[2]string]) {
	counterMap = make(map[string]int64)
	totalProcessed = 0
	keyPrefix = ""

	for _, entry := range config.Slice() {
		if entry[0] == "key_prefix" {
			keyPrefix = entry[1]
		}
	}

	store = kv.NewStore("counter-store")
	storeInit = true
}

func fsProcess(sourceID uint32, data cm.List[uint8]) {
	inputStr := strings.TrimSpace(string(data.Slice()))
	if inputStr == "" {
		return
	}

	totalProcessed++

	fullKey := keyPrefix + inputStr
	result := store.GetState(cm.ToList([]byte(fullKey)))

	currentCount := int64(0)
	if result.OK() != nil {
		opt := result.OK()
		if optVal := opt.Some(); optVal != nil {
			valueBytes := *optVal
			if count, err := strconv.ParseInt(string(valueBytes.Slice()), 10, 64); err == nil {
				currentCount = count
			}
		}
	}

	newCount := currentCount + 1
	counterMap[inputStr] = newCount

	newCountStr := strconv.FormatInt(newCount, 10)
	store.PutState(cm.ToList([]byte(fullKey)), cm.ToList([]byte(newCountStr)))

	outputPayload := map[string]interface{}{
		"total_processed": totalProcessed,
		"counter_map":     counterMap,
	}

	if jsonBytes, err := json.Marshal(outputPayload); err == nil {
		collector.Emit(0, cm.ToList(jsonBytes))
	}
}

func fsProcessWatermark(sourceID uint32, watermark uint64) {
	collector.EmitWatermark(0, watermark)
}

func fsTakeCheckpoint(checkpointID uint64) {}

func fsCheckHeartbeat() bool {
	return true
}

func fsClose() {
	if storeInit {
		store.ResourceDrop()
		storeInit = false
	}
	counterMap = nil
}

func fsExec(className string, modules cm.List[cm.Tuple[string, cm.List[uint8]]]) {}

func fsCustom(payload cm.List[uint8]) cm.List[uint8] {
	return payload
}

func main() {}
