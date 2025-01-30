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
	ResourceProvider[model.Function]
}

type PackageStorage interface {
	ResourceProvider[model.Package]
}

type ResourceProvider[T any] interface {
	Create(ctx context.Context, r *T) error
	Read(ctx context.Context, name string) (*T, error)
	Upsert(ctx context.Context, r *T) error
	Delete(ctx context.Context, name string) error
	List(ctx context.Context) ([]*T, error)
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
	StateStore() StateStore
}

type RuntimeAdapter interface {
	DeployFunction(ctx context.Context, instance Instance) error
	DeleteFunction(ctx context.Context, name string) error
}

type StateStore interface {
	Get(ctx context.Context, key string) ([]byte, error)
	Put(ctx context.Context, key string, value []byte) error
	List(ctx context.Context, startInclusive string, endExclusive string) ([]string, error)
	Delete(ctx context.Context, key string) error
}
