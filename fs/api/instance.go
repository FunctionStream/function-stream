package api

import (
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/contube"
	"golang.org/x/net/context"
)

type FunctionInstance interface {
	Context() context.Context
	Definition() *model.Function
	Stop()
	Run(factory FunctionRuntimeFactory)
	WaitForReady() <-chan error
}

type FunctionInstanceFactory interface {
	NewFunctionInstance(f *model.Function, queueFactory contube.TubeFactory, i int32) FunctionInstance
}
