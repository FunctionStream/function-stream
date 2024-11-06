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
	ctx         *FunctionContext
	processFunc func(context.Context, []byte) []byte // Only for DefFunction
	executeFunc func(context.Context) error
	initFunc    func(context.Context) error
	registerErr error
}

func (m *moduleWrapper) AddInitFunc(initFunc func(context.Context) error) *moduleWrapper {
	parentInit := m.initFunc
	if parentInit != nil {
		m.initFunc = func(ctx context.Context) error {
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

func RegisterFunction[I any, O any](function Function[I, O]) *moduleWrapper {
	m := &moduleWrapper{}
	processFunc := func(ctx context.Context, payload []byte) []byte {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
			return nil
		}
		output, err := function.Handle(m.ctx, NewEvent(input))
		// TODO: Handle err
		outputPayload, _ := json.Marshal(output.Data())
		return outputPayload
	}
	m.initFunc = func(ctx context.Context) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return function.Init(m.ctx)
	}
	m.executeFunc = func(ctx context.Context) error {
		for {
			inputPayload, err := m.rpc.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			outputPayload := processFunc(ctx, inputPayload)
			err = m.rpc.Write(ctx, outputPayload)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to write: %s\n", err)
			}
		}
	}
	m.processFunc = processFunc
	return m
}

func RegisterSource[O any](source Source[O]) *moduleWrapper {
	m := &moduleWrapper{}
	emit := func(ctx context.Context, event Event[O]) error {
		outputPayload, _ := json.Marshal(event.Data())
		return m.rpc.Write(m.ctx.warpContext(ctx), outputPayload)
	}
	m.initFunc = func(ctx context.Context) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return source.Init(m.ctx)
	}
	m.executeFunc = func(ctx context.Context) error {
		return source.Handle(m.ctx, emit)
	}
	return m
}

func RegisterSink[I any](sink Sink[I]) *moduleWrapper {
	m := &moduleWrapper{}
	processFunc := func(ctx context.Context, payload []byte) {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		sink.Handle(m.ctx, NewEvent(input))
	}
	m.initFunc = func(ctx context.Context) error {
		inputSchema, err := avroschema.Reflect(new(I))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, inputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return sink.Init(m.ctx)
	}
	m.executeFunc = func(ctx context.Context) error {
		for {
			inputPayload, err := m.rpc.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			processFunc(ctx, inputPayload)
		}
	}
	return m
}

func RegisterCustom(custom Custom) *moduleWrapper {
	m := &moduleWrapper{}
	initFunc := func(ctx context.Context) error {
		return custom.Init(m.ctx)
	}
	executeFunc := func(ctx context.Context) error {
		return custom.Handle(m.ctx)
	}
	m.initFunc = initFunc
	m.executeFunc = executeFunc

	// TODO: Simplify this
	return m
}

func DefFunction[I any, O any](process func(context.Context, *I) *O) *moduleWrapper {
	processFunc := func(ctx context.Context, payload []byte) []byte {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		output := process(ctx, input)
		outputPayload, _ := json.Marshal(output)
		return outputPayload
	}
	m := &moduleWrapper{}
	m.initFunc = func(ctx context.Context) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func(ctx context.Context) error {
		for {
			inputPayload, err := m.rpc.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			outputPayload := processFunc(ctx, inputPayload)
			err = m.rpc.Write(ctx, outputPayload)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to write: %s\n", err)
			}
		}
	}
	m.processFunc = processFunc
	return m
}

func DefSource[O any](process func(ctx context.Context, emit func(context.Context, *O) error)) *moduleWrapper {
	m := &moduleWrapper{}
	emit := func(ctx context.Context, event *O) error {
		outputPayload, _ := json.Marshal(event)
		return m.rpc.Write(ctx, outputPayload)
	}
	m.initFunc = func(ctx context.Context) error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func(ctx context.Context) error {
		process(ctx, emit)
		return nil
	}
	return m
}

func DefSink[I any](process func(context.Context, *I)) *moduleWrapper {
	processFunc := func(ctx context.Context, payload []byte) {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		process(ctx, input)
	}
	m := &moduleWrapper{}
	m.initFunc = func(ctx context.Context) error {
		inputSchema, err := avroschema.Reflect(new(I))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(ctx, inputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func(ctx context.Context) error {
		for {
			inputPayload, err := m.rpc.Read(ctx)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			processFunc(ctx, inputPayload)
		}
	}
	return m
}

func DefCustom(init func(ctx context.Context) error, execute func(ctx context.Context) error) *moduleWrapper {
	return &moduleWrapper{
		initFunc:    init,
		executeFunc: execute,
	}
}

type FunctionContext struct {
	c      *fsClient
	name   string
	module string
}

func (c *FunctionContext) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.c.rpc.GetState(c.warpContext(ctx), key)
}

func (c *FunctionContext) PutState(ctx context.Context, key string, value []byte) error {
	return c.c.rpc.PutState(c.warpContext(ctx), key, value)
}

func (c *FunctionContext) Write(ctx context.Context, payload []byte) error {
	return c.c.rpc.Write(c.warpContext(ctx), payload)
}

func (c *FunctionContext) Read(ctx context.Context) ([]byte, error) {
	return c.c.rpc.Read(c.warpContext(ctx))
}

func (c *FunctionContext) GetConfig(ctx context.Context) (map[string]string, error) {
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
	funcCtx := &FunctionContext{c: c, name: funcName, module: module}
	if c.rpc == nil {
		rpc, err := newFSRPCClient()
		if err != nil {
			return err
		}
		c.rpc = rpc
	}
	ctx := funcCtx.warpContext(context.WithValue(context.Background(), funcCtxKey{}, funcCtx))
	m.fsClient = c
	m.ctx = funcCtx
	err := m.initFunc(ctx)
	if err != nil {
		return err
	}
	c.rpc.loadModule(m)
	if c.rpc.skipExecuting() {
		return nil
	}
	return m.executeFunc(ctx)
}

func (c *fsClient) Error() string {
	return c.err.Error()
}
