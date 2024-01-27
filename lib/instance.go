package lib

import (
	"context"
	"encoding/binary"
	"encoding/json"
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
}

func NewFunctionInstance(definition model.Function) *FunctionInstance {
	return &FunctionInstance{
		ctx:        context.TODO(),
		definition: definition,
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

	// Test
	p := Person{Name: "rbt", Money: 0}
	jsonBytes, err := json.Marshal(p)
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error marshalling json", err)
		return
	}

	lengthBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(lengthBytes, uint64(len(jsonBytes)))
	inCh <- lengthBytes
	inCh <- jsonBytes
	close(inCh)

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

	lengthBytes = make([]byte, 8)
	output := common.NewChanReader(outCh)
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

	var p2 Person
	err = json.Unmarshal(dataBytes, &p2)
	if err != nil {
		slog.ErrorContext(instance.ctx, "Error unmarshalling json", err)
		return
	}

	slog.InfoContext(instance.ctx, "Function output", "output", p2)
}
