package grpc_func

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
	pb "github.com/functionstream/functionstream/fs/func/grpc_func/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"testing"
	"time"
)

func TestGRPCFunc(t *testing.T) {
	fsService := NewFSReconcile()
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

	// Create a client
	client := pb.NewFSReconcileClient(conn)

	ctx := context.Background()
	stream, err := client.Reconcile(ctx)
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

	select {
	case <-fsService.WaitForReady():
		t.Logf("ready")
	case <-time.After(1 * time.Second):
		t.Fatal("timeout waiting for fs service ready")
		return
	}

	go func() {
		for {
			event, err := stream.Recv()
			if err != nil {
				t.Errorf("failed to receive: %v", err)
				return
			}
			t.Logf("client received status: %v", event)
			event.Status = pb.FunctionStatus_RUNNING
			err = stream.Send(event)
			if err != nil {
				t.Errorf("failed to send: %v", err)
				return
			}
		}
	}()

	function, err := fsService.NewFunction(&model.Function{
		Name: "test",
	})
	if err != nil {
		t.Error(err)
		return
	}
	select {
	case <-function.WaitForReady():
		t.Logf("function ready")
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for function ready")
	}
}
