package lib

import (
	"context"
	"errors"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
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
	readyCh    chan bool
}

func NewFunctionInstance(definition model.Function, pc pulsar.Client) *FunctionInstance {
	ctx, cancelFunc := context.WithCancel(context.Background())
	return &FunctionInstance{
		ctx:        ctx,
		cancelFunc: cancelFunc,
		definition: definition,
		pc:         pc,
		readyCh:    make(chan bool),
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
		slog.ErrorContext(instance.ctx, "Error instantiating function module", err)
		return
	}

	stdin := common.NewChanReader()
	stdout := common.NewChanWriter()

	config := wazero.NewModuleConfig().
		WithStdout(stdout).WithStderr(os.Stderr).WithStdin(stdin)

	wasi_snapshot_preview1.MustInstantiate(instance.ctx, r)

	wasmBytes, err := os.ReadFile(instance.definition.Archive)
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error reading wasm file", err)
		return
	}

	consumer, err := instance.pc.Subscribe(pulsar.ConsumerOptions{
		Topics:           instance.definition.Inputs,
		SubscriptionName: fmt.Sprintf("function-stream-%s", instance.definition.Name),
	})
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error creating consumer", err)
		return
	}
	defer func() {
		consumer.Close()
	}()

	producer, err := instance.pc.CreateProducer(pulsar.ProducerOptions{
		Topic: instance.definition.Output,
	})
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error creating producer", err)
		return
	}
	defer func() {
		producer.Close()
	}()

	instance.readyCh <- true

	handleErr := func(ctx context.Context, err error, message string, args ...interface{}) {
		if errors.Is(err, context.Canceled) {
			slog.InfoContext(instance.ctx, "function instance has been stopped")
			return
		}
		slog.ErrorContext(ctx, message, args...)
	}

	for {
		msg, err := consumer.Receive(instance.ctx)
		if err != nil {
			handleErr(instance.ctx, err, "Error receiving message")
			return
		}
		stdin.ResetBuffer(msg.Payload())

		// Trigger the "_start" function, WASI's "main".
		_, err = r.InstantiateWithConfig(instance.ctx, wasmBytes, config)
		if err != nil {
			if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
				handleErr(instance.ctx, err, "Function exit with code", "code", exitErr.ExitCode())
			} else if !ok {
				handleErr(instance.ctx, err, "Error instantiating function")
			}
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

func (instance *FunctionInstance) WaitForReady() {
	<-instance.readyCh
}

func (instance *FunctionInstance) Stop() {
	instance.cancelFunc()
}
