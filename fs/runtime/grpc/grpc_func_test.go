package grpc

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/contube"
	"github.com/functionstream/functionstream/fs/runtime/grpc/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"io"
	"testing"
	"time"
)

func TestGRPCFunc(t *testing.T) {
	ctx, closeFSReconcile := context.WithCancel(context.Background())
	fsService := NewFSReconcile(ctx)
	go func() {
		err := StartGRPCServer(fsService)
		if err != nil {
			t.Errorf("did not start: %v", err)
			return
		}
	}()
	addr := "localhost:7400"
	// Set up a connection to the server.
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("did not connect: %v", err)
	}
	defer func(conn *grpc.ClientConn) {
		err := conn.Close()
		if err != nil {
			t.Fatalf("did not close: %v", err)
			return
		}
	}(conn)
	client := proto.NewFSReconcileClient(conn)

	stream, err := client.Reconcile(context.Background())
	if err != nil {
		t.Fatalf("failed to get process stream: %v", err)
	}
	defer func() {
		err := stream.CloseSend()
		if err != nil {
			t.Fatalf("failed to close: %v", err)
			return
		}
	}()

	funcCli := proto.NewFunctionClient(conn)

	select {
	case <-fsService.WaitForReady():
		t.Logf("ready")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for fs service ready")
		return
	}

	go func() {
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

	funcCtx, funcCancel := context.WithCancel(context.Background())
	function, err := fsService.NewFunctionRuntime(funcCtx, &model.Function{
		Name: "test",
	})
	if err != nil {
		t.Error(err)
	}
	select {
	case <-function.WaitForReady():
		t.Logf("function ready")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for function ready")
	}

	result, err := function.Call(contube.NewRecordImpl([]byte("hello"), func() {
		t.Logf("commit")
	}))
	if err != nil {
		t.Fatalf("failed to call function: %v", err)
	}
	if string(result.GetPayload()) != "hello!" {
		t.Fatalf("unexpected result: %v", result)
	}

	funcCancel()
	function.Stop()

	fsService.functionsMu.Lock()
	if _, ok := fsService.functions["test"]; ok {
		t.Fatalf("function not removed")
	}
	fsService.functionsMu.Unlock()

	closeFSReconcile()

	time.Sleep(3 * time.Second) // Wait for some time to make sure the cleanup of function doesn't raise any errors
}
