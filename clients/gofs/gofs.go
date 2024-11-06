/*
 * Copyright 2024 Function Stream Org.
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

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/wirelessr/avroschema"
)

const (
	StateInit int32 = iota
	StateRunning
)

const (
	FSSocketPath   = "FS_SOCKET_PATH"
	FSFunctionName = "FS_FUNCTION_NAME"
	FSModuleName   = "FS_MODULE_NAME"
	DefaultModule  = "default"
)

var (
	ErrRegisterModuleDuringRunning = fmt.Errorf("cannot register module during running")
	ErrAlreadyRunning              = fmt.Errorf("already running")
)

type FSClient interface {
	error
	Register(module string, wrapper *moduleWrapper) FSClient
	Run() error
}

type fsClient struct {
	rpc        *fsRPCClient
	modules    map[string]*moduleWrapper
	state      int32
	registerMu sync.Mutex
	err        error
}

func NewFSClient() FSClient {
	return &fsClient{
		modules: make(map[string]*moduleWrapper),
		state:   StateInit,
	}
}

type moduleWrapper struct {
	*fsClient
	ctx         *functionContextImpl
	processFunc func(FunctionContext, []byte) ([]byte, error) // Only for Wasm Function
	executeFunc func(FunctionContext) error
	initFunc    func(FunctionContext) error
	registerErr error
}

func (m *moduleWrapper) AddInitFunc(initFunc func(FunctionContext) error) *moduleWrapper {
	parentInit := m.initFunc
	if parentInit != nil {
		m.initFunc = func(ctx FunctionContext) error {
			err := parentInit(ctx)
			if err != nil {
				return err
			}
			return initFunc(ctx)
		}
	} else {
		m.initFunc = initFunc
	}
	return m
}

func (c *fsClient) Register(module string, wrapper *moduleWrapper) FSClient {
	if c.err != nil {
		return c
	}
	c.registerMu.Lock()
	defer c.registerMu.Unlock()
	if c.state == StateRunning {
		c.err = ErrRegisterModuleDuringRunning
		return c
	}
	if wrapper.registerErr != nil {
		c.err = wrapper.registerErr
		return c
	}
	c.modules[module] = wrapper
	return c
}

func WithFunction[I any, O any](function Function[I, O]) *moduleWrapper {
	m := &moduleWrapper{}
	processFunc := func(ctx FunctionContext, payload []byte) ([]byte, error) {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			return nil, fmt.Errorf("failed to parse JSON: %w", err)
		}
		output, err := function.Handle(ctx, NewEvent(input))
		if err != nil {
			return nil, err
		}
		outputPayload, err := json.Marshal(output.Data())
		if err != nil {
			return nil, fmt.Errorf("failed to marshal JSON: %w", err)
		}
		return outputPayload, nil
	}
	m.initFunc = func(ctx FunctionContext) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return function.Init(ctx)
	}
	m.executeFunc = func(ctx FunctionContext) error {
		for {
			inputPayload, err := ctx.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			outputPayload, err := processFunc(ctx, inputPayload)
			if err != nil {
				return err
			}
			err = ctx.Write(ctx, outputPayload)
			if err != nil {
				return err
			}
		}
	}
	m.processFunc = processFunc
	return m
}

func WithSource[O any](source Source[O]) *moduleWrapper {
	m := &moduleWrapper{}
	emit := func(ctx context.Context, event Event[O]) error {
		outputPayload, _ := json.Marshal(event.Data())
		return m.ctx.Write(ctx, outputPayload)
	}
	m.initFunc = func(ctx FunctionContext) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return source.Init(ctx)
	}
	m.executeFunc = func(ctx FunctionContext) error {
		return source.Handle(ctx, emit)
	}
	return m
}

func WithSink[I any](sink Sink[I]) *moduleWrapper {
	m := &moduleWrapper{}
	processFunc := func(ctx FunctionContext, payload []byte) error {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			return fmt.Errorf("failed to parse JSON: %w", err)
		}
		return sink.Handle(ctx, NewEvent(input))
	}
	m.initFunc = func(ctx FunctionContext) error {
		inputSchema, err := avroschema.Reflect(new(I))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, inputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return sink.Init(ctx)
	}
	m.executeFunc = func(ctx FunctionContext) error {
		for {
			inputPayload, err := ctx.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			if err = processFunc(ctx, inputPayload); err != nil {
				return err
			}
		}
	}
	return m
}

func WithCustom(custom Custom) *moduleWrapper {
	m := &moduleWrapper{}
	initFunc := func(ctx FunctionContext) error {
		return custom.Init(ctx)
	}
	executeFunc := func(ctx FunctionContext) error {
		return custom.Handle(ctx)
	}
	m.initFunc = initFunc
	m.executeFunc = executeFunc

	// TODO: Simplify this
	return m
}

type functionContextImpl struct {
	context.Context
	c      *fsClient
	name   string
	module string
}

func (c *functionContextImpl) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.c.rpc.GetState(c.warpContext(ctx), key)
}

func (c *functionContextImpl) PutState(ctx context.Context, key string, value []byte) error {
	return c.c.rpc.PutState(c.warpContext(ctx), key, value)
}

func (c *functionContextImpl) Write(ctx context.Context, payload []byte) error {
	return c.c.rpc.Write(c.warpContext(ctx), payload)
}

func (c *functionContextImpl) Read(ctx context.Context) ([]byte, error) {
	return c.c.rpc.Read(c.warpContext(ctx))
}

func (c *functionContextImpl) GetConfig(ctx context.Context) (map[string]string, error) {
	return c.c.rpc.GetConfig(c.warpContext(ctx))
}

type funcCtxKey struct{}

func GetFunctionContext(ctx context.Context) *FunctionContext {
	return ctx.Value(funcCtxKey{}).(*FunctionContext)
}

func (c *fsClient) Run() error {
	if c.err != nil {
		return c.err
	}
	c.registerMu.Lock()
	if c.state == StateRunning {
		c.registerMu.Unlock()
		return ErrAlreadyRunning
	}
	c.state = StateRunning
	c.registerMu.Unlock()

	funcName := os.Getenv(FSFunctionName)
	if funcName == "" {
		return fmt.Errorf("%s is not set", FSFunctionName)
	}
	module := os.Getenv(FSModuleName)
	if module == "" {
		module = DefaultModule
	}
	m, ok := c.modules[module]
	if !ok {
		return fmt.Errorf("module %s not found", module)
	}
	funcCtx := &functionContextImpl{c: c, name: funcName, module: module}
	if c.rpc == nil {
		rpc, err := newFSRPCClient()
		if err != nil {
			return err
		}
		c.rpc = rpc
	}
	ctx := funcCtx.warpContext(context.WithValue(context.Background(), funcCtxKey{}, funcCtx))
	funcCtx.Context = ctx
	m.fsClient = c
	m.ctx = funcCtx
	err := m.initFunc(funcCtx)
	if err != nil {
		return err
	}
	c.rpc.loadModule(m)
	if c.rpc.skipExecuting() {
		return nil
	}
	return m.executeFunc(funcCtx)
}

func (c *fsClient) Error() string {
	return c.err.Error()
}
