//go:build !wasi
// +build !wasi

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
	"fmt"
	"os"

	"github.com/functionstream/function-stream/fs/runtime/external/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

type fsRPCClient struct {
	ctx     context.Context
	grpcCli model.FunctionClient
}

func newFSRPCClient() (*fsRPCClient, error) {
	socketPath := os.Getenv(FSSocketPath)
	if socketPath == "" {
		return nil, fmt.Errorf("%s is not set", FSSocketPath)
	}
	funcName := os.Getenv(FSFunctionName)
	if funcName == "" {
		return nil, fmt.Errorf("%s is not set", FSFunctionName)
	}

	serviceConfig := `{
		   "methodConfig": [{
		       "name": [{"service": "*"}],
		       "retryPolicy": {
		           "maxAttempts": 30,
		           "initialBackoff": "0.1s",
		           "maxBackoff": "30s",
		           "backoffMultiplier": 2,
		           "retryableStatusCodes": ["UNAVAILABLE"]
		       }
		   }]
		}`
	conn, err := grpc.NewClient(
		"unix:"+socketPath,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithDefaultServiceConfig(serviceConfig),
	)
	if err != nil {
		return nil, err
	}
	client := model.NewFunctionClient(conn)
	md := metadata.New(map[string]string{
		"name": funcName,
	})
	ctx := metadata.NewOutgoingContext(context.Background(), md)
	return &fsRPCClient{grpcCli: client, ctx: ctx}, nil
}

func (c *fsRPCClient) RegisterSchema(schema string) error {
	_, err := c.grpcCli.RegisterSchema(c.ctx, &model.RegisterSchemaRequest{Schema: schema})
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	return nil
}

func (c *fsRPCClient) Write(payload []byte) error {
	_, err := c.grpcCli.Write(c.ctx, &model.Event{Payload: payload})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}
	return nil
}

func (c *fsRPCClient) Read() ([]byte, error) {
	res, err := c.grpcCli.Read(c.ctx, &model.ReadRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	return res.Payload, nil
}

func (c *fsRPCClient) loadModule(_ *moduleWrapper) {
	// no-op
}

func (c *fsRPCClient) skipExecuting() bool {
	return false
}
