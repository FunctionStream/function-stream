package api

import (
	"github.com/functionstream/functionstream/fs/contube"
)

type FunctionRuntime interface {
	WaitForReady() <-chan error
	Call(e contube.Record) (contube.Record, error)
	Stop()
}

type FunctionRuntimeFactory interface {
	NewFunctionRuntime(instance FunctionInstance) (FunctionRuntime, error)
}
