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

package grpc

import (
	"errors"
	"io"
	"log/slog"
	"strconv"
	"testing"

	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/runtime/grpc/proto"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func StartMockGRPCFunc(t *testing.T, addr string) {
	// Set up a connection to the server.
	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("did not connect: %v", err)
		return
	}
	client := proto.NewFSReconcileClient(conn)

	stream, err := client.Reconcile(context.Background(), &proto.ConnectRequest{})
	if err != nil {
		t.Errorf("failed to get process stream: %v", err)
		return
	}

	funcCli := proto.NewFunctionClient(conn)

	go func() {
		defer func(conn *grpc.ClientConn) {
			err := conn.Close()
			if err != nil {
				t.Errorf("did not close: %v", err)
				return
			}
		}(conn)
		defer func() {
			err := stream.CloseSend()
			if err != nil {
				t.Errorf("failed to close: %v", err)
				return
			}
		}()
		for {
			s, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				slog.Info("server disconnected: %v", err)
				return
			}
			t.Logf("client received status: %v", s)
			s.Status = proto.FunctionStatus_RUNNING
			res, err := client.UpdateStatus(context.Background(), s)
			if err != nil {
				t.Errorf("failed to send: %v", err)
				return
			}
			if res.GetStatus() != proto.Response_OK {
				t.Errorf("failed to update status: %s", res.GetMessage())
			}
			go func() {
				ctx := metadata.AppendToOutgoingContext(context.Background(), "name", s.Name)
				processStream, err := funcCli.Process(ctx, &proto.FunctionProcessRequest{
					Name: s.Name,
				})
				if err != nil {
					t.Errorf("failed to get process stream: %v", err)
					return
				}
				for {
					event, err := processStream.Recv()
					if err != nil {
						slog.Info("server disconnected: %v", err)
						return
					}
					t.Logf("client received event: %v", event)
					event.Payload += "!"

					counter, err := funcCli.GetState(ctx, &proto.GetStateRequest{
						Key: "counter",
					})
					if err == nil {
						count, err := strconv.Atoi(string(counter.Value))
						assert.Nil(t, err)
						count += 1
						res, err = funcCli.PutState(ctx, &proto.PutStateRequest{
							Key:   "counter",
							Value: []byte(strconv.Itoa(count)),
						})
						assert.Nil(t, err)
						assert.Equal(t, proto.Response_OK, res.Status)
					} else if !errors.Is(err, api.ErrNotFound) {
						t.Errorf("failed to get state: %v", err)
					}

					res, err := funcCli.Output(ctx, event)
					if err != nil {
						t.Errorf("failed to send event: %v", err)
						return
					}
					if res.GetStatus() != proto.Response_OK {
						t.Errorf("failed to send event: %s", res.GetMessage())
						return
					}
				}
			}()
		}
	}()
}
