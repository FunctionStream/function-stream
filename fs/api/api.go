package api

import (
	"context"
	"github.com/functionstream/function-stream/fs/model"
)

type Manager interface {
	Deploy(ctx context.Context, f *model.Function) error
	Delete(ctx context.Context, name string) error
}

type PackageLoader interface {
	LoadPackage(ctx context.Context, name string) (*model.Package, error)
}

type Instance interface {
	Context() context.Context
	Function() *model.Function
	Package() *model.Package
}

type RuntimeAdapter interface {
	DeployFunction(ctx context.Context, instance Instance) error
	DeleteFunction(ctx context.Context, name string) error
}
