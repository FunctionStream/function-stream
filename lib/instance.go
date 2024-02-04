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
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/pkg/errors"
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
	definition model.Function
	pc         pulsar.Client
	readyCh    chan error
}

func NewFunctionInstance(definition model.Function, pc pulsar.Client) *FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &FunctionInstance{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		definition: definition,
		pc:         pc,
		readyCh:    make(chan error),
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

	consumer, err := instance.pc.Subscribe(pulsar.ConsumerOptions{
		Topics:           instance.definition.Inputs,
		SubscriptionName: fmt.Sprintf("function-stream-%s", instance.definition.Name),
	})
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating consumer")
		return
	}
	defer func() {
		consumer.Close()
	}()

	producer, err := instance.pc.CreateProducer(pulsar.ProducerOptions{
		Topic: instance.definition.Output,
	})
	if err != nil {
		instance.readyCh <- errors.Wrap(err, "Error creating producer")
		return
	}
	defer func() {
		producer.Close()
	}()

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

	instance.readyCh <- nil

	for {
		msg, err := consumer.Receive(instance.ctx)
		if err != nil {
			handleErr(instance.ctx, err, "Error receiving message")
			return
		}
		stdin.ResetBuffer(msg.Payload())

		_, err = process.Call(instance.ctx)
		if err != nil {
			handleErr(instance.ctx, err, "Error calling process function")
			return
		}

		output := stdout.GetAndReset()
		producer.SendAsync(instance.ctx, &pulsar.ProducerMessage{
			Payload: output,
		}, func(id pulsar.MessageID, message *pulsar.ProducerMessage, err error) {
			if err != nil {
				handleErr(instance.ctx, err, "Error sending message", "error", err, "messageId", id)
				return
			}
		})
	}
}

func (instance *FunctionInstance) WaitForReady() error {
	err := <-instance.readyCh
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error starting function instance", err)
	}
	return err
}

func (instance *FunctionInstance) Stop() {
	instance.cancelFunc()
}
