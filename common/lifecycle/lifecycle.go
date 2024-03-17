package lifecycle

import (
	"github.com/functionstream/function-stream/common"
	"golang.org/x/net/context"
)

type Lifecycle struct {
	closer    common.Closer
	parent    *Lifecycle
	ctx       context.Context
	closeFunc func() error
}

type LifecycleOption func(*Lifecycle)

func NewLifecycle(opts ...LifecycleOption) *Lifecycle {
	l := &Lifecycle{}
	for _, opt := range opts {
		opt(l)
	}
	if l.closer == nil {
		l.closer = common.NewCloser(l.ctx, l.closeFunc)
	}
	if l.parent != nil {
		l.parent.closer.AddChildCloser(l.closer)
	}
	return l
}

func WithParent(parent *Lifecycle) LifecycleOption {
	return func(l *Lifecycle) {
		l.parent = parent
		l.ctx = parent.ctx
	}
}

func WithCloseFunc(f func() error) LifecycleOption {
	return func(l *Lifecycle) {
		l.closeFunc = f
	}
}

func WithContext(ctx context.Context) LifecycleOption {
	return func(l *Lifecycle) {
		l.ctx = ctx
	}
}

func (l *Lifecycle) GetLifecycle() *Lifecycle {
	return l
}

func (l *Lifecycle) Context() context.Context {
	return l.ctx
}

func (l *Lifecycle) AddCloseHook(name string) (<-chan struct{}, func(err error)) {
	return l.closer.AddCloseHook(name)
}

func (l *Lifecycle) CheckState() error {
	return l.closer.Err()
}

func (l *Lifecycle) Close() error {
	return l.closer.Close()
}
