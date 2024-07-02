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
	"os"

	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/tetratelabs/wazero"
	wazero_api "github.com/tetratelabs/wazero/api"
	exp_sys "github.com/tetratelabs/wazero/experimental/sys"
	"github.com/tetratelabs/wazero/experimental/sysfs"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
)

type WazeroFunctionRuntimeFactory struct {
}

type WASMFetcher interface {
	Fetch(url string) ([]byte, error)
}

type FileWASMFetcher struct {
}

func (f *FileWASMFetcher) Fetch(url string) ([]byte, error) {
	return os.ReadFile(url)
}

func NewWazeroFunctionRuntimeFactory() api.FunctionRuntimeFactory {
	return NewWazeroFunctionRuntimeFactoryWithOptions(WithWASMFetcher(&FileWASMFetcher{}))
}

func NewWazeroFunctionRuntimeFactoryWithOptions(opts ...func(*options)) api.FunctionRuntimeFactory {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}
	return &WazeroFunctionRuntimeFactory{}
}

type options struct {
	wasmFetcher WASMFetcher
}

func WithWASMFetcher(fetcher WASMFetcher) func(*options) {
	return func(o *options) {
		o.wasmFetcher = fetcher
	}
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
	wasmLog := &logWriter{
		log: log,
	}

	processFile := &oneShotFile{}
	registerSchema := &oneShotFile{}
	fileMap := map[string]exp_sys.File{
		"process":        processFile,
		"registerSchema": registerSchema,
	}
	fsConfig := wazero.NewFSConfig().(sysfs.FSConfig).WithSysFSMount(newMemoryFS(fileMap), "")
	config := wazero.NewModuleConfig().
		WithStdout(wasmLog).WithStderr(wasmLog).WithFSConfig(fsConfig)

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
	process := mod.ExportedFunction("process")
	if process == nil {
		return nil, fmt.Errorf("no process function found")
	}
	outputSchemaDef := registerSchema.output
	var outputSchema string
	if outputSchemaDef != nil {
		outputSchema = string(outputSchemaDef)
		log.Info("Register the output schema", "schema", outputSchema)
	}
	return &FunctionRuntime{
		callFunc: func(e contube.Record) (contube.Record, error) {
			processFile.input = e.GetPayload()
			_, err := process.Call(instance.Context())
			if err != nil {
				return nil, err
			}
			return contube.NewSchemaRecordImpl(processFile.output, outputSchema, e.Commit), nil
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
