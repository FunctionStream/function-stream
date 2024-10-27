//go:build wasi
// +build wasi

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

import "C"
import (
	"context"
	"fmt"
	"os"
	"syscall"
)

var processFd int
var registerSchemaFd int

func init() {
	processFd, _ = syscall.Open("/process", syscall.O_RDWR, 0)
	registerSchemaFd, _ = syscall.Open("/registerSchema", syscall.O_RDWR, 0)
}

var runningModule *moduleWrapper

//export process
func process() {
	if runningModule == nil {
		panic("no module loaded")
	}
	err := runningModule.executeFunc(context.Background())
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
}

func (c *FunctionContext) warpContext(parent context.Context) context.Context {
	return parent
}

type fsRPCClient struct {
}

func newFSRPCClient() (*fsRPCClient, error) {
	return &fsRPCClient{}, nil
}

func (c *fsRPCClient) RegisterSchema(ctx context.Context, schema string) error {
	_, err := syscall.Write(registerSchemaFd, []byte(schema))
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	return nil
}

func (c *fsRPCClient) Write(ctx context.Context, payload []byte) error {
	panic("rpc write not supported")
}

func (c *fsRPCClient) Read(ctx context.Context) ([]byte, error) {
	panic("rpc read not supported")
}

func (c *fsRPCClient) GetState(ctx context.Context, key string) ([]byte, error) {
	panic("rpc get state not supported")
}

func (c *fsRPCClient) PutState(ctx context.Context, key string, value []byte) error {
	panic("rpc put state not supported")
}

func (c *fsRPCClient) GetConfig(ctx context.Context) (map[string]string, error) {
	panic("rpc get config not supported")
}

func (c *fsRPCClient) loadModule(m *moduleWrapper) {
	if m.processFunc == nil {
		panic("only function module is supported for the wasm runtime")
	}
	m.executeFunc = func(ctx context.Context) error {
		var stat syscall.Stat_t
		syscall.Fstat(processFd, &stat)
		payload := make([]byte, stat.Size)
		_, err := syscall.Read(processFd, payload)
		if err != nil {
			return fmt.Errorf("failed to read: %w", err)
		}
		outputPayload := m.processFunc(ctx, payload)
		_, err = syscall.Write(processFd, outputPayload)
		if err != nil {
			return fmt.Errorf("failed to write: %w", err)
		}
		return nil
	}
	runningModule = m
}

func (c *fsRPCClient) skipExecuting() bool {
	return true
}
