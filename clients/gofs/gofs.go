/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gofs

import "C"
import (
	"encoding/json"
	. "github.com/functionstream/function-stream/common/wasm_utils"
	"github.com/wirelessr/avroschema"
)

var processFunc func(uint64) uint64

//go:wasmimport fs registerSchema
func registerSchema(inputSchemaPtrSize, outputSchemaPtrSize uint64)

func Register[I any, O any](process func(*I) *O) error {
	inputSchema, err := avroschema.Reflect(new(I))
	if err != nil {
		return err
	}
	outputSchema, err := avroschema.Reflect(new(O))
	if err != nil {
		return err
	}
	processFunc = func(ptrSize uint64) uint64 {
		payload := PtrToString(ExtractPtrSize(ptrSize))
		input := new(I)
		err = json.Unmarshal([]byte(payload), input)
		if err != nil {
			//fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s", err, payload)
		}
		output := process(input)
		outputPayload, _ := json.Marshal(output)
		return PtrSize(StringToPtr(string(outputPayload)))
	}
	registerSchema(PtrSize(StringToPtr(inputSchema)), PtrSize(StringToPtr(outputSchema)))
	return nil
}

//export processRecord
func processRecord(ptrSize uint64) uint64 {
	return processFunc(ptrSize)
}
