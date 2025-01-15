package api

import (
	"context"
	"github.com/functionstream/function-stream/fs/model"
	"io"
)

type Event interface {
	ID() string
	SchemaID() int64
	Payload() io.Reader
	Properties() map[string]string
	Commit(ctx context.Context) error
}

type Manager interface {
	Deploy(ctx context.Context, f *model.Function) error
	Delete(ctx context.Context, name string) error
}

type PackageLoader interface {
	LoadPackage(ctx context.Context, name string) (*model.Package, error)
}

type EventStorage interface {
	Read(ctx context.Context, topics []model.TopicConfig) (<-chan Event, error)
	Write(ctx context.Context, event Event, topic model.TopicConfig) error
	Commit(ctx context.Context, eventId string) error
}

type Instance interface {
	Context() context.Context
	Function() *model.Function
	Package() *model.Package
	EventStorage() EventStorage
}

type RuntimeAdapter interface {
	DeployFunction(ctx context.Context, instance Instance) error
	DeleteFunction(ctx context.Context, name string) error
}
