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
	"github.com/functionstream/functionstream/fs/runtime/grpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"testing"
)

func StartMockGRPCFunc(t *testing.T) {
	addr := "localhost:7400"
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Errorf("did not connect: %v", err)
		return
	}
	client := proto.NewFSReconcileClient(conn)

	stream, err := client.Reconcile(context.Background())
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
				t.Errorf("failed to receive: %v", err)
				return
			}
			t.Logf("client received status: %v", s)
			s.Status = proto.FunctionStatus_RUNNING
			err = stream.Send(s)
			if err != nil {
				t.Errorf("failed to send: %v", err)
				return
			}
			go func() {
				ctx := metadata.AppendToOutgoingContext(context.Background(), "name", "test")
				processStream, err := funcCli.Process(ctx)
				if err != nil {
					t.Errorf("failed to get process stream: %v", err)
					return
				}
				for {
					event, err := processStream.Recv()
					if err == io.EOF {
						return
					}
					if err != nil {
						t.Errorf("failed to receive event: %v", err)
						return
					}
					t.Logf("client received event: %v", event)
					event.Payload += "!"
					err = processStream.Send(event)
					if err != nil {
						t.Errorf("failed to send event: %v", err)
						return
					}
				}
			}()
		}
	}()
}
