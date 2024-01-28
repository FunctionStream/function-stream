package lib

import (
	"context"
	"encoding/binary"
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
	definition model.Function
	pc         pulsar.Client
	readyCh    chan bool
}

func NewFunctionInstance(definition model.Function, pc pulsar.Client) *FunctionInstance {
	return &FunctionInstance{
		ctx:        context.TODO(),
		definition: definition,
		pc:         pc,
		readyCh:    make(chan bool),
	}
}

type Person struct {
	Name  string `json:"name"`
	Money int    `json:"money"`
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

	inCh := make(chan []byte, 100)
	stdin := common.NewChanReader(inCh)

	outCh := make(chan []byte, 100)
	stdout := common.NewChanWriter(outCh)

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

	producer, err := instance.pc.CreateProducer(pulsar.ProducerOptions{
		Topic: instance.definition.Output,
	})
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error creating producer", err)
		return

	}

	go func() {
		for {
			msg, err := consumer.Receive(instance.ctx)
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error receiving message", err)
				return
			}
			payload := msg.Payload()
			lengthBytes := make([]byte, 8)
			binary.BigEndian.PutUint64(lengthBytes, uint64(len(payload)))
			inCh <- lengthBytes
			inCh <- payload
		}
	}()

	go func() {
		output := common.NewChanReader(outCh)
		for {
			lengthBytes := make([]byte, 8)
			_, err = output.Read(lengthBytes)
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error reading length", err)
				return
			}

			length := binary.BigEndian.Uint64(lengthBytes)
			dataBytes := make([]byte, length)
			_, err = output.Read(dataBytes)
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error reading data", err)
				return
			}
			_, err = producer.Send(instance.ctx, &pulsar.ProducerMessage{
				Payload: dataBytes,
			})
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error sending message", err)
				return
			}
		}
	}()

	instance.readyCh <- true

	// InstantiateModule runs the "_start" function, WASI's "main".
	_, err = r.InstantiateWithConfig(instance.ctx, wasmBytes, config)
	if err != nil {
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			slog.ErrorContext(instance.ctx, "Function exit with code", "code", exitErr.ExitCode())
			return
		} else if !ok {
			slog.ErrorContext(instance.ctx, "Error instantiating function", err)
			return
		}
	}
}

func (instance *FunctionInstance) WaitForReady() {
	<-instance.readyCh
}
