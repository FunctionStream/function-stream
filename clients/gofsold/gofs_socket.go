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

package gofsold

import (
	"context"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc/metadata"

	"github.com/functionstream/function-stream/fsold/runtime/external/model"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func (c *functionContextImpl) warpContext(parent context.Context) context.Context {
	return metadata.NewOutgoingContext(parent, metadata.New(map[string]string{
		"name": c.name,
	}))
}

func passMetadataContext(from context.Context, to context.Context) context.Context {
	md, ok := metadata.FromOutgoingContext(from)
	if ok {
		return metadata.NewOutgoingContext(to, md)
	}
	return to
}

type fsRPCClient struct {
	grpcCli model.FunctionClient
}

func newFSRPCClient() (*fsRPCClient, error) {
	socketPath := os.Getenv(FSSocketPath)
	if socketPath == "" {
		return nil, fmt.Errorf("%s is not set", FSSocketPath)
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
	return &fsRPCClient{grpcCli: client}, nil
}

func (c *fsRPCClient) RegisterSchema(ctx context.Context, schema string) error {
	_, err := c.grpcCli.RegisterSchema(ctx, &model.RegisterSchemaRequest{Schema: schema})
	if err != nil {
		return fmt.Errorf("failed to register schema: %w", err)
	}
	return nil
}

func (c *fsRPCClient) Write(ctx context.Context, event Event[[]byte]) error {
	_, err := c.grpcCli.Write(ctx, &model.Event{Payload: *event.Data()})
	if err != nil {
		return fmt.Errorf("failed to write: %w", err)
	}
	return event.Ack(ctx)
}

func (c *fsRPCClient) Read(ctx context.Context) (Event[[]byte], error) {
	res, err := c.grpcCli.Read(ctx, &model.ReadRequest{})
	if err != nil {
		return nil, fmt.Errorf("failed to read: %w", err)
	}
	return NewEventWithAck(&res.Payload, func(ackCtx context.Context) error {
		if _, err := c.grpcCli.Ack(passMetadataContext(ctx, ackCtx), &model.AckRequest{
			Id: res.Id,
		}); err != nil {
			return err
		}
		return nil
	}), nil
}

func (c *fsRPCClient) PutState(ctx context.Context, key string, value []byte) error {
	_, err := c.grpcCli.PutState(ctx, &model.PutStateRequest{Key: key, Value: value})
	if err != nil {
		return err
	}
	return nil
}

func (c *fsRPCClient) GetState(ctx context.Context, key string) ([]byte, error) {
	res, err := c.grpcCli.GetState(ctx, &model.GetStateRequest{Key: key})
	if err != nil {
		return nil, err
	}
	return res.Value, nil
}

func (c *fsRPCClient) ListStates(ctx context.Context, path string) ([]string, error) {
	path = strings.TrimSuffix(path, "/")
	startInclusive := path + "/"
	endExclusive := path + "//"
	res, err := c.grpcCli.ListStates(ctx, &model.ListStatesRequest{StartInclusive: startInclusive,
		EndExclusive: endExclusive})
	if err != nil {
		return nil, err
	}
	return res.Keys, nil
}

func (c *fsRPCClient) GetConfig(ctx context.Context) (map[string]string, error) {
	res, err := c.grpcCli.GetConfig(ctx, &model.GetConfigRequest{})
	if err != nil {
		return nil, err
	}
	return res.Config, nil
}

func (c *fsRPCClient) loadModule(_ *moduleWrapper) {
	// no-op
}

func (c *fsRPCClient) skipExecuting() bool {
	return false
}
