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
	err := runningModule.executeFunc()
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
}

type fsRPCClient struct {
}

func newFSRPCClient() (*fsRPCClient, error) {
	return &fsRPCClient{}, nil
}

func (c *fsRPCClient) RegisterSchema(schema string) error {
	_, err := syscall.Write(registerSchemaFd, []byte(schema))
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	return nil
}

func (c *fsRPCClient) Write(payload []byte) error {
	panic("rpc write not implemented")
}

func (c *fsRPCClient) Read() ([]byte, error) {
	panic("rpc read not implemented")
}

func (c *fsRPCClient) loadModule(m *moduleWrapper) {
	m.executeFunc = func() error {
		var stat syscall.Stat_t
		syscall.Fstat(processFd, &stat)
		payload := make([]byte, stat.Size)
		_, err := syscall.Read(processFd, payload)
		if err != nil {
			return fmt.Errorf("failed to read: %w", err)
		}
		outputPayload := m.processFunc(payload)
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
