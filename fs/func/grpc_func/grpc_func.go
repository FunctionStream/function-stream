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

package grpc_func

import (
	"fmt"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/fs/contube"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"io"
	"log/slog"
	"net"
	"sync"

	pb "github.com/functionstream/functionstream/fs/func/grpc_func/proto"
)

var okResponse *pb.Response

func init() {
	okResponse = &pb.Response{Status: pb.Response_OK}
}

// TODO: Replace with FunctionInstane after the function instance abstraction is finishedf
type GRPCFuncRuntime struct {
	Name     string
	ctx      context.Context
	status   *pb.FunctionStatus
	readyCh  chan error
	input    chan string
	output   chan string
	stopFunc func()
}

// FSSReconcileServer is the struct that implements the FSServiceServer interface.
type FSSReconcileServer struct {
	pb.UnimplementedFSReconcileServer
	ctx         context.Context
	readyCh     chan struct{}
	connected   sync.Once
	reconcile   chan *pb.FunctionStatus
	functions   map[string]*GRPCFuncRuntime
	functionsMu sync.Mutex
}

func NewFSReconcile(ctx context.Context) *FSSReconcileServer {
	return &FSSReconcileServer{
		ctx:       ctx,
		readyCh:   make(chan struct{}),
		reconcile: make(chan *pb.FunctionStatus, 100),
		functions: make(map[string]*GRPCFuncRuntime),
	}
}

func (s *FSSReconcileServer) WaitForReady() <-chan struct{} {
	return s.readyCh
}

func (s *FSSReconcileServer) Reconcile(stream pb.FSReconcile_ReconcileServer) error {
	s.connected.Do(func() {
		close(s.readyCh)
	})
	errCh := make(chan error)
	go func() {
		for {
			newStatus, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			s.functionsMu.Lock()
			instance, ok := s.functions[newStatus.Name]
			if !ok {
				s.functionsMu.Unlock()
				slog.Error("receive non-exist function status update", slog.Any("name", newStatus.Name))
				continue
			}
			s.functionsMu.Unlock()
			instance.Update(newStatus)
		}
	}()
	for {
		select {
		case status := <-s.reconcile:
			err := stream.Send(status)
			if err != nil {
				slog.ErrorContext(stream.Context(), "failed to send status update", slog.Any("status", status))
				// Continue to send the next status update.
			}
		case <-stream.Context().Done():
			return nil
		case <-s.ctx.Done():
			return nil
		case e := <-errCh:
			return e
		}
	}

}

func (s *FSSReconcileServer) NewRuntime(ctx context.Context, f *model.Function) (*GRPCFuncRuntime, error) {
	instance := &GRPCFuncRuntime{
		Name:    f.Name,
		readyCh: make(chan error),
		input:   make(chan string),
		output:  make(chan string),
		status: &pb.FunctionStatus{
			Name:   f.Name,
			Status: pb.FunctionStatus_CREATING,
		},
		ctx: ctx,
		stopFunc: func() {
			s.removeFunction(f.Name)
		},
	}
	{
		s.functionsMu.Lock()
		defer s.functionsMu.Unlock()
		if _, ok := s.functions[f.Name]; ok {
			return nil, fmt.Errorf("function already exists")
		}
		s.functions[f.Name] = instance
	}
	s.reconcile <- instance.status
	slog.InfoContext(instance.ctx, "Creating function", slog.Any("name", f.Name))
	return instance, nil
}

func (s *FSSReconcileServer) getFunc(name string) (*GRPCFuncRuntime, error) {
	s.functionsMu.Lock()
	defer s.functionsMu.Unlock()
	instance, ok := s.functions[name]
	if !ok {
		return nil, fmt.Errorf("function not found")
	}
	return instance, nil
}

func (s *FSSReconcileServer) removeFunction(name string) {
	s.functionsMu.Lock()
	defer s.functionsMu.Unlock()
	instance, ok := s.functions[name]
	if !ok {
		return
	}
	slog.InfoContext(instance.ctx, "Removing function", slog.Any("name", name))
	delete(s.functions, name)
}

func isFinalStatus(status pb.FunctionStatus_Status) bool {
	return status == pb.FunctionStatus_FAILED || status == pb.FunctionStatus_DELETED
}

func (f *GRPCFuncRuntime) Update(new *pb.FunctionStatus) {
	if f.status.Status == pb.FunctionStatus_CREATING && isFinalStatus(new.Status) {
		f.readyCh <- fmt.Errorf("function failed to start")
	}
	if f.status.Status != new.Status {
		slog.InfoContext(f.ctx, "Function status update", slog.Any("new_status", new.Status), slog.Any("old_status", f.status.Status))
	}
	f.status = new
}

func (f *GRPCFuncRuntime) WaitForReady() <-chan error {
	return f.readyCh
}

// Stop stops the function runtime and remove it
// It is different from the ctx.Cancel. It will make sure the runtime has been deleted after this method returns.
func (f *GRPCFuncRuntime) Stop() {
	f.stopFunc()
}

func (f *GRPCFuncRuntime) Call(event contube.Record) (contube.Record, error) {
	f.input <- string(event.GetPayload())
	out := <-f.output
	return contube.NewRecordImpl([]byte(out), event.Commit), nil
}

type FunctionServerImpl struct {
	pb.UnimplementedFunctionServer
	reconcileSvr *FSSReconcileServer
}

func NewFunctionServerImpl(s *FSSReconcileServer) *FunctionServerImpl {
	return &FunctionServerImpl{
		reconcileSvr: s,
	}
}

func (f *FunctionServerImpl) Process(stream pb.Function_ProcessServer) error {
	md, ok := metadata.FromIncomingContext(stream.Context())
	if !ok {
		return fmt.Errorf("failed to get metadata")
	}
	instance, err := f.reconcileSvr.getFunc(md["name"][0])
	if err != nil {
		return err
	}
	slog.InfoContext(stream.Context(), "Start processing events", slog.Any("name", md["name"]))
	instance.readyCh <- err
	errCh := make(chan error)
	go func() {
		for {
			event, err := stream.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				errCh <- err
				return
			}
			instance.output <- event.Payload
		}
	}()

	for {
		select {
		case payload := <-instance.input:
			err := stream.Send(&pb.Event{Payload: payload})
			if err != nil {
				slog.Error("failed to send event", slog.Any("error", err))
				return err
			}
		case <-stream.Context().Done():
			return nil
		case <-instance.ctx.Done():
			return nil
		case e := <-errCh:
			return e
		}
	}
}

func StartGRPCServer(f *FSSReconcileServer) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7400))
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterFSReconcileServer(s, f)
	pb.RegisterFunctionServer(s, NewFunctionServerImpl(f))
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}
