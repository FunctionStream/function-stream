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

package wazero

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/wasm_utils"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/tetratelabs/wazero"
	wazero_api "github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
	"os"
)

type WazeroFunctionRuntimeFactory struct {
}

func NewWazeroFunctionRuntimeFactory() api.FunctionRuntimeFactory {
	return &WazeroFunctionRuntimeFactory{}
}

func (f *WazeroFunctionRuntimeFactory) NewFunctionRuntime(instance api.FunctionInstance) (api.FunctionRuntime, error) {
	log := instance.Logger()
	r := wazero.NewRuntime(instance.Context())
	_, err := r.NewHostModuleBuilder("env").NewFunctionBuilder().WithFunc(func(ctx context.Context,
		m wazero_api.Module, a, b, c, d uint32) {
		log.Error(fmt.Errorf("abort(%d, %d, %d, %d)", a, b, c, d), "the function is calling abort")
	}).Export("abort").Instantiate(instance.Context())
	if err != nil {
		return nil, fmt.Errorf("error instantiating env module: %w", err)
	}
	stdin := common.NewChanReader()
	stdout := common.NewChanWriter()

	config := wazero.NewModuleConfig().
		WithStdout(stdout).WithStdin(stdin).WithStderr(os.Stderr)

	wasi_snapshot_preview1.MustInstantiate(instance.Context(), r)

	if instance.Definition().Runtime.Config == nil {
		return nil, fmt.Errorf("no runtime config found")
	}
	path, exist := instance.Definition().Runtime.Config["archive"]
	if !exist {
		return nil, fmt.Errorf("no wasm archive found")
	}
	pathStr := path.(string)
	if pathStr == "" {
		return nil, fmt.Errorf("empty wasm archive found")
	}
	wasmBytes, err := os.ReadFile(pathStr)
	if err != nil {
		return nil, fmt.Errorf("error reading wasm file: %w", err)
	}
	var outputSchemaDef string
	_, err = r.NewHostModuleBuilder("fs").
		NewFunctionBuilder().
		WithFunc(func(ctx context.Context, m wazero_api.Module, inputSchema uint64, outputSchema uint64) {
			inputBuf, ok := m.Memory().Read(wasm_utils.ExtractPtrSize(inputSchema))
			if !ok {
				log.Error(fmt.Errorf("failed to read memory"), "failed to read memory")
				return
			}
			if log.DebugEnabled() {
				log.Info("Register the input schema", "schema", string(inputBuf))
			}
			outputBuf, ok := m.Memory().Read(wasm_utils.ExtractPtrSize(outputSchema))
			if !ok {
				log.Error(fmt.Errorf("failed to read memory"), "failed to read memory")
				return
			}
			if log.DebugEnabled() {
				log.Info("Register the output schema", "schema", string(outputBuf))
			}
			outputSchemaDef = string(outputBuf)
		}).Export("registerSchema").
		Instantiate(instance.Context())
	if err != nil {
		return nil, fmt.Errorf("error creating fs module: %w", err)
	}
	// Trigger the "_start" function, WASI's "main".
	mod, err := r.InstantiateWithConfig(instance.Context(), wasmBytes, config)
	if err != nil {
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			return nil, fmt.Errorf("failed to instantiate function, function exit with code %d", exitErr.ExitCode())
		} else if !ok {
			return nil, fmt.Errorf("failed to instantiate function: %w", err)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("error instantiating runtime: %w", err)
	}
	malloc := mod.ExportedFunction("malloc")
	free := mod.ExportedFunction("free")
	processRecord := mod.ExportedFunction("processRecord")
	if processRecord == nil {
		return nil, fmt.Errorf("no process function found")
	}
	return &FunctionRuntime{
		callFunc: func(e contube.Record) (contube.Record, error) {
			payload := e.GetPayload()
			payloadSize := len(payload)
			mallocResults, err := malloc.Call(instance.Context(), uint64(payloadSize))
			if err != nil {
				panic(err)
			}
			ptr := mallocResults[0]
			defer func() {
				_, err := free.Call(instance.Context(), ptr)
				if err != nil {
					panic(err)
				}
			}()
			if !mod.Memory().Write(uint32(ptr), payload) {
				log.Error(fmt.Errorf("Memory.Write(%d, %d) out of range of memory size %d",
					ptr, payloadSize, mod.Memory().Size()), "failed to write memory")
			}
			outputPtrSize, err := processRecord.Call(instance.Context(), wasm_utils.PtrSize(uint32(ptr), uint32(payloadSize)))
			if err != nil {
				panic(err)
			}
			//fmt.Println(wasm_utils.ExtractPtrSize(outputPtrSize[0]))
			if bytes, ok := mod.Memory().Read(wasm_utils.ExtractPtrSize(outputPtrSize[0])); !ok {
				return nil, fmt.Errorf("failed to read memory")
			} else {
				return contube.NewSchemaRecordImpl(bytes, outputSchemaDef, e.Commit), nil
			}
		},
		stopFunc: func() {
			err := r.Close(instance.Context())
			if err != nil {
				log.Error(err, "failed to close the runtime")
			}
		},
		log: log,
	}, nil
}

type FunctionRuntime struct {
	api.FunctionRuntime
	callFunc func(e contube.Record) (contube.Record, error)
	stopFunc func()
	log      *common.Logger
}

func (r *FunctionRuntime) Call(e contube.Record) (contube.Record, error) {
	return r.callFunc(e)
}

func (r *FunctionRuntime) Stop() {
	r.stopFunc()
}
