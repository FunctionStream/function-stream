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
	processFunc func([]byte) []byte // Only for Function
	executeFunc func() error
	initFunc    func() error
	registerErr error
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

func Function[I any, O any](process func(*I) *O) *moduleWrapper {
	processFunc := func(payload []byte) []byte {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		output := process(input)
		outputPayload, _ := json.Marshal(output)
		return outputPayload
	}
	m := &moduleWrapper{}
	m.initFunc = func() error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func() error {
		for {
			inputPayload, err := m.rpc.Read()
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			outputPayload := processFunc(inputPayload)
			err = m.rpc.Write(outputPayload)
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to write: %s\n", err)
			}
		}
	}
	m.processFunc = processFunc
	return m
}

func Source[O any](process func(emit func(*O) error)) *moduleWrapper {
	m := &moduleWrapper{}
	emit := func(event *O) error {
		outputPayload, _ := json.Marshal(event)
		return m.rpc.Write(outputPayload)
	}
	m.initFunc = func() error {
		outputSchema, err := avroschema.Reflect(new(O))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(outputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func() error {
		process(emit)
		return nil
	}
	return m
}

func Sink[I any](process func(*I)) *moduleWrapper {
	processFunc := func(payload []byte) {
		input := new(I)
		err := json.Unmarshal(payload, input)
		if err != nil {
			_, _ = fmt.Fprintf(os.Stderr, "Failed to parse JSON: %s %s\n", err, payload)
		}
		process(input)
	}
	m := &moduleWrapper{}
	m.initFunc = func() error {
		inputSchema, err := avroschema.Reflect(new(I))
		if err != nil {
			return err
		}
		err = m.rpc.RegisterSchema(inputSchema)
		if err != nil {
			return fmt.Errorf("failed to register schema: %w", err)
		}
		return nil
	}
	m.executeFunc = func() error {
		for {
			inputPayload, err := m.rpc.Read()
			if err != nil {
				_, _ = fmt.Fprintf(os.Stderr, "Failed to read: %s\n", err)
				time.Sleep(3 * time.Second)
				continue
			}
			processFunc(inputPayload)
		}
	}
	return m
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

	if c.rpc == nil {
		rpc, err := newFSRPCClient()
		if err != nil {
			return err
		}
		c.rpc = rpc
	}
	module := os.Getenv(FSModuleName)
	if module == "" {
		module = DefaultModule
	}
	m, ok := c.modules[module]
	if !ok {
		return fmt.Errorf("module %s not found", module)
	}
	m.fsClient = c
	err := m.initFunc()
	if err != nil {
		return err
	}
	c.rpc.loadModule(m)
	if c.rpc.skipExecuting() {
		return nil
	}
	return m.executeFunc()
}

func (c *fsClient) Error() string {
	return c.err.Error()
}
