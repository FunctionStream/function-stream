package api

import (
	"context"
)

const DefaultModule = "default"

type Event[T any] interface {
	Data() *T
	Commit(ctx context.Context) error
}

type FunctionContext interface {
	context.Context
	GetState(ctx context.Context, key string) ([]byte, error)
	PutState(ctx context.Context, key string, value []byte) error
	Write(ctx context.Context, rawEvent Event[[]byte]) error
	Read(ctx context.Context) (<-chan Event[[]byte], error)
	GetConfig() map[string]string
}

type BaseModule interface {
	Init(ctx FunctionContext) error
}

type Function[I any, O any] interface {
	BaseModule
	Handle(ctx FunctionContext, event Event[I]) (Event[O], error)
}

type Source[O any] interface {
	BaseModule
	Handle(ctx FunctionContext, emit func(context.Context, Event[O]) error) error
}

type Sink[I any] interface {
	BaseModule
	Handle(ctx FunctionContext, event Event[I]) error
}

type Custom interface {
	BaseModule
	Handle(ctx FunctionContext) error
}

type eventImpl[T any] struct {
	data       *T
	commitFunc func(context.Context) error
}

func NewEvent[T any](data *T) Event[T] {
	return NewEventWithCommit(data, nil)
}

func NewEventWithCommit[T any](data *T, ack func(ctx context.Context) error) Event[T] {
	return &eventImpl[T]{
		data:       data,
		commitFunc: ack,
	}
}

func (e *eventImpl[T]) Data() *T {
	return e.data
}

func (e *eventImpl[T]) Commit(ctx context.Context) error {
	if e.commitFunc != nil {
		return e.commitFunc(ctx)
	}
	return nil
}

type simpleFunction[I any, O any] struct {
	handle func(ctx FunctionContext, event Event[I]) (Event[O], error)
}

func NewSimpleFunction[I any, O any](handle func(ctx FunctionContext, event Event[I]) (Event[O], error)) Function[I, O] {
	return &simpleFunction[I, O]{
		handle: handle,
	}
}

func (f *simpleFunction[I, O]) Init(_ FunctionContext) error {
	return nil
}

func (f *simpleFunction[I, O]) Handle(ctx FunctionContext, event Event[I]) (Event[O], error) {
	return f.handle(ctx, event)
}

type simpleSource[O any] struct {
	handle func(ctx FunctionContext, emit func(context.Context, Event[O]) error) error
}

func NewSimpleSource[O any](handle func(ctx FunctionContext, emit func(context.Context, Event[O]) error) error) Source[O] {
	return &simpleSource[O]{
		handle: handle,
	}
}

func (s *simpleSource[O]) Init(_ FunctionContext) error {
	return nil
}

func (s *simpleSource[O]) Handle(ctx FunctionContext, emit func(context.Context, Event[O]) error) error {
	return s.handle(ctx, emit)
}

type simpleSink[I any] struct {
	handle func(ctx FunctionContext, event Event[I]) error
}

func NewSimpleSink[I any](handle func(ctx FunctionContext, event Event[I]) error) Sink[I] {
	return &simpleSink[I]{
		handle: handle,
	}
}

func (s *simpleSink[I]) Init(_ FunctionContext) error {
	return nil
}

func (s *simpleSink[I]) Handle(ctx FunctionContext, event Event[I]) error {
	return s.handle(ctx, event)
}

type simpleCustom struct {
	handle func(ctx FunctionContext) error
}

func NewSimpleCustom(handle func(ctx FunctionContext) error) Custom {
	return &simpleCustom{
		handle: handle,
	}
}

func (c *simpleCustom) Init(_ FunctionContext) error {
	return nil
}

func (c *simpleCustom) Handle(ctx FunctionContext) error {
	return c.handle(ctx)
}
