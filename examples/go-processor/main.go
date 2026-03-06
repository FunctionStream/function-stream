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
	fssdk "github.com/functionstream/function-stream/go-sdk"
	"strconv"
	"strings"
)

func init() {
	fssdk.Run(&CounterProcessor{})
}

type CounterProcessor struct {
	fssdk.BaseDriver
	store          fssdk.Store
	counterMap     map[string]int64
	totalProcessed int64
	keyPrefix      string
}

func (p *CounterProcessor) Init(ctx fssdk.Context, config map[string]string) error {
	store, err := ctx.GetOrCreateStore("counter-store")
	if err != nil {
		return err
	}

	p.store = store
	p.counterMap = make(map[string]int64)
	p.totalProcessed = 0
	p.keyPrefix = strings.TrimSpace(config["key_prefix"])
	return nil
}

func (p *CounterProcessor) Process(ctx fssdk.Context, sourceID uint32, data []byte) error {
	_ = ctx
	_ = sourceID
	inputStr := strings.TrimSpace(string(data))
	if inputStr == "" {
		return nil
	}

	if p.store == nil {
		return nil
	}

	p.totalProcessed++

	fullKey := p.keyPrefix + inputStr
	existing, found, err := p.store.GetState([]byte(fullKey))
	if err != nil {
		return err
	}

	currentCount := int64(0)
	if found {
		if count, parseErr := strconv.ParseInt(string(existing), 10, 64); parseErr == nil {
			currentCount = count
		}
	}

	newCount := currentCount + 1
	p.counterMap[inputStr] = newCount

	newCountStr := strconv.FormatInt(newCount, 10)
	if err = p.store.PutState([]byte(fullKey), []byte(newCountStr)); err != nil {
		return err
	}

	outputPayload := map[string]interface{}{
		"total_processed": p.totalProcessed,
		"counter_map":     p.counterMap,
	}

	jsonBytes, err := json.Marshal(outputPayload)
	if err != nil {
		return err
	}
	return ctx.Emit(0, jsonBytes)
}

func (p *CounterProcessor) ProcessWatermark(ctx fssdk.Context, sourceID uint32, watermark uint64) error {
	_ = sourceID
	return ctx.EmitWatermark(0, watermark)
}

func (p *CounterProcessor) Close(ctx fssdk.Context) error {
	_ = ctx
	p.store = nil
	p.counterMap = nil
	p.totalProcessed = 0
	p.keyPrefix = ""
	return nil
}

func main() {
}
