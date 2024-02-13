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

package lib

import (
	"context"
	"fmt"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib/contube"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
	"log/slog"
	"os"
)

type FunctionInstance struct {
	ctx          context.Context
	cancelFunc   context.CancelFunc
	definition   *model.Function
	queueFactory contube.TubeFactory
	readyCh      chan error
	index        int32
}

func NewFunctionInstance(definition *model.Function, queueFactory contube.TubeFactory, index int32) *FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	ctx.Value(logrus.Fields{
		"function-name":  definition.Name,
		"function-index": index,
	})
	return &FunctionInstance{
		ctx:          ctx,
		cancelFunc:   cancelFunc,
		definition:   definition,
		queueFactory: queueFactory,
		readyCh:      make(chan error),
		index:        index,
	}
}

func (instance *FunctionInstance) Run() {
	r := wazero.NewRuntime(instance.ctx)
	defer func(runtime wazero.Runtime, ctx context.Context) {
		err := runtime.Close(ctx)
		if err != nil {
			slog.ErrorContext(ctx, "Error closing r", err)
		}
	}(r, instance.ctx)

	_, err := r.NewHostModuleBuilder("env").NewFunctionBuilder().WithFunc(func(ctx context.Context, m api.Module, a, b, c, d uint32) {
		panic("abort")
	}).Export("abort").Instantiate(instance.ctx)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error instantiating function module")
		return
	}

	stdin := common.NewChanReader()
	stdout := common.NewChanWriter()

	config := wazero.NewModuleConfig().
		WithStdout(stdout).WithStdin(stdin).WithStderr(os.Stderr)

	wasi_snapshot_preview1.MustInstantiate(instance.ctx, r)

	wasmBytes, err := os.ReadFile(instance.definition.Archive)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error reading wasm file")
		return
	}

	handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
		if errors.Is(err, context.Canceled) {
			slog.InfoContext(instance.ctx, "function instance has been stopped")
			return
		}
		extraArgs := append(args, slog.Any("error", err.Error()))
		slog.ErrorContext(ctx, message, extraArgs...)
	}

	// Trigger the "_start" function, WASI's "main".
	mod, err := r.InstantiateWithConfig(instance.ctx, wasmBytes, config)
	if err != nil {
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			handleErr(instance.ctx, err, "Function exit with code", "code", exitErr.ExitCode())
		} else if !ok {
			handleErr(instance.ctx, err, "Error instantiating function")
		}
		return
	}
	process := mod.ExportedFunction("process")
	if process == nil {
		instance.readyCh <- errors.New("No process function found")
		return
	}

	sourceChan, err := instance.queueFactory.NewSourceTube(instance.ctx, (&contube.SourceQueueConfig{Topics: instance.definition.Inputs, SubName: fmt.Sprintf("function-stream-%s", instance.definition.Name)}).ToConfigMap())
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating source event queue")
		return
	}
	sinkChan, err := instance.queueFactory.NewSinkTube(instance.ctx, (&contube.SinkQueueConfig{Topic: instance.definition.Output}).ToConfigMap())
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating sink event queue")
		return
	}
	defer close(sinkChan)

	instance.readyCh <- nil
	for e := range sourceChan {
		stdin.ResetBuffer(e.GetPayload())
		_, err = process.Call(instance.ctx)
		if err != nil {
			handleErr(instance.ctx, err, "Error calling process function")
			return
		}
		output := stdout.GetAndReset()
		sinkChan <- contube.NewRecordImpl(output, e.Commit)
	}
}

func (instance *FunctionInstance) WaitForReady() <-chan error {
	return instance.readyCh
}

func (instance *FunctionInstance) Stop() {
	instance.cancelFunc()
}
