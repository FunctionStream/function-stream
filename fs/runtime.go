package fs

import (
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/fs/contube"
	"github.com/pkg/errors"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/tetratelabs/wazero/imports/wasi_snapshot_preview1"
	"github.com/tetratelabs/wazero/sys"
	"golang.org/x/net/context"
	"log/slog"
	"os"
)

type FunctionRuntime interface {
	WaitForReady() <-chan error
	Call(e contube.Record) (contube.Record, error)
	Stop()
}

type FunctionRuntimeFactory interface {
	NewFunctionRuntime(instance *FunctionInstanceImpl) (FunctionRuntime, error)
}

type WazeroFunctionRuntimeFactory struct {
}

func NewWazeroFunctionRuntimeFactory() *WazeroFunctionRuntimeFactory {
	return &WazeroFunctionRuntimeFactory{}
}

func (f *WazeroFunctionRuntimeFactory) NewFunctionRuntime(instance *FunctionInstanceImpl) (FunctionRuntime, error) {
	r := wazero.NewRuntime(instance.ctx)
	_, err := r.NewHostModuleBuilder("env").NewFunctionBuilder().WithFunc(func(ctx context.Context, m api.Module, a, b, c, d uint32) {
		panic("abort")
	}).Export("abort").Instantiate(instance.ctx)
	if err != nil {
		return nil, errors.Wrap(err, "Error instantiating function module")
	}
	stdin := common.NewChanReader()
	stdout := common.NewChanWriter()

	config := wazero.NewModuleConfig().
		WithStdout(stdout).WithStdin(stdin).WithStderr(os.Stderr)

	wasi_snapshot_preview1.MustInstantiate(instance.ctx, r)

	wasmBytes, err := os.ReadFile(instance.definition.Archive)
	if err != nil {
		return nil, errors.Wrap(err, "Error reading wasm file")
	}
	// Trigger the "_start" function, WASI's "main".
	mod, err := r.InstantiateWithConfig(instance.ctx, wasmBytes, config)
	if err != nil {
		if exitErr, ok := err.(*sys.ExitError); ok && exitErr.ExitCode() != 0 {
			handleErr(instance.ctx, err, "Function exit with code", "code", exitErr.ExitCode())
		} else if !ok {
			return nil, errors.Wrap(err, "Error instantiating function")
		}
	}
	process := mod.ExportedFunction("process")
	if process == nil {
		return nil, errors.New("No process function found")
	}
	return &WazeroFunctionRuntime{
		instance: instance,
		callFunc: func(e contube.Record) (contube.Record, error) {
			stdin.ResetBuffer(e.GetPayload())
			_, err := process.Call(instance.ctx)
			if err != nil {
				return nil, errors.Wrap(err, "Error calling wasm function")
			}
			output := stdout.GetAndReset()
			return contube.NewRecordImpl(output, e.Commit), nil
		},
		stopFunc: func() {
			err := r.Close(instance.ctx)
			if err != nil {
				slog.ErrorContext(instance.ctx, "Error closing r", err)
			}
		},
	}, nil
}

type WazeroFunctionRuntime struct {
	instance *FunctionInstanceImpl
	callFunc func(e contube.Record) (contube.Record, error)
	stopFunc func()
}

func (r *WazeroFunctionRuntime) WaitForReady() <-chan error {
	c := make(chan error)
	close(c)
	return c
}

func (r *WazeroFunctionRuntime) Call(e contube.Record) (contube.Record, error) {
	return r.callFunc(e)
}

func (r *WazeroFunctionRuntime) Stop() {
	r.stopFunc()
}
