/*
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
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
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
	ctx        context.Context
	cancelFunc context.CancelFunc
	definition *model.Function
	newQueue   EventQueueFactory
	readyCh    chan error
	index      int32
}

func NewFunctionInstance(definition *model.Function, queueFactory EventQueueFactory, index int32) *FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	ctx.Value(logrus.Fields{
		"function-name":  definition.Name,
		"function-index": index,
	})
	return &FunctionInstance{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		definition: definition,
		newQueue:   queueFactory,
		readyCh:    make(chan error),
		index:      index,
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

	queue, err := instance.newQueue(instance.ctx, &QueueConfig{Inputs: instance.definition.Inputs, Output: instance.definition.Output}, instance.definition)
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating event queue")
		return
	}

	handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
		if errors.Is(err, context.Canceled) {
			slog.InfoContext(instance.ctx, "function instance has been stopped")
			return
		}
		slog.ErrorContext(ctx, message, args...)
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
	recvChan, err := queue.GetRecvChan()
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error getting recv channel")
		return
	}
	sendChan, err := queue.GetSendChan()
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error getting send channel")
		return
	}

	instance.readyCh <- nil
	for e := range recvChan {
		payload, ackFunc := e()
		stdin.ResetBuffer(payload)
		_, err = process.Call(instance.ctx)
		if err != nil {
			handleErr(instance.ctx, err, "Error calling process function")
			return
		}
		output := stdout.GetAndReset()
		sendChan <- func() ([]byte, func()) {
			return output, ackFunc
		}
	}
}

func (instance *FunctionInstance) WaitForReady() <-chan error {
	return instance.readyCh
}

func (instance *FunctionInstance) Stop() {
	instance.cancelFunc()
}
