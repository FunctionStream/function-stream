//go:build wasi
// +build wasi

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
	"fmt"
	"github.com/wirelessr/avroschema"
	"os"
	"syscall"
)

var processFd int
var registerSchemaFd int

func init() {
	processFd, _ = syscall.Open("/process", syscall.O_RDWR, 0)
	registerSchemaFd, _ = syscall.Open("/registerSchema", syscall.O_RDWR, 0)
}

var processFunc func([]byte) []byte

func Register[I any, O any](process func(*I) *O) error {
	outputSchema, err := avroschema.Reflect(new(O))
	if err != nil {
		return err
	}
	processFunc = func(payload []byte) []byte {
		input := new(I)
		err = json.Unmarshal(payload, input)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s", err, payload)
		}
		output := process(input)
		outputPayload, _ := json.Marshal(output)
		return outputPayload
	}
	syscall.Write(registerSchemaFd, []byte(outputSchema))
	return nil
}

//export process
func process() {
	var stat syscall.Stat_t
	syscall.Fstat(processFd, &stat)
	payload := make([]byte, stat.Size)
	_, _ = syscall.Read(processFd, payload)
	outputPayload := processFunc(payload)
	_, _ = syscall.Write(processFd, outputPayload)
}

func Run() {
	// Leave it empty
}
