/*
 * Copyright 2024 DefFunction Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package gofs

import "context"

type FunctionContext interface {
	context.Context
	GetState(ctx context.Context, key string) ([]byte, error)
	PutState(ctx context.Context, key string, value []byte) error
	Write(ctx context.Context, payload []byte) error
	Read(ctx context.Context) ([]byte, error)
	GetConfig(ctx context.Context) (map[string]string, error)
}

type Event[T any] interface {
	Data() *T
	Ack() error // TODO: Handle Ack
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
	data *T
}

func NewEvent[T any](data *T) Event[T] {
	return &eventImpl[T]{
		data: data,
	}
}

func (e *eventImpl[T]) Data() *T {
	return e.data
}

func (e *eventImpl[T]) Ack() error {
	// TODO: Implement this
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
