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
type FuncInstance struct {
	Name        string
	status      *pb.FunctionStatus
	readyCh     chan error
	readyNotify sync.Once
	input       chan string
	output      chan string
}

// FSSReconcileServer is the struct that implements the FSServiceServer interface.
type FSSReconcileServer struct {
	pb.UnimplementedFSReconcileServer
	readyCh     chan struct{}
	connected   sync.Once
	reconcile   chan *pb.FunctionStatus
	functions   map[string]*FuncInstance
	functionsMu sync.Mutex
}

func NewFSReconcile() *FSSReconcileServer {
	return &FSSReconcileServer{
		readyCh:   make(chan struct{}),
		reconcile: make(chan *pb.FunctionStatus, 100),
		functions: make(map[string]*FuncInstance),
	}
}

func (s *FSSReconcileServer) WaitForReady() <-chan struct{} {
	return s.readyCh
}

func (s *FSSReconcileServer) Reconcile(stream pb.FSReconcile_ReconcileServer) error {
	s.connected.Do(func() {
		close(s.readyCh)
	})
	go func() {
		for {
			select {
			case status := <-s.reconcile:
				err := stream.Send(status)
				if err != nil {
					slog.ErrorContext(stream.Context(), "failed to send status update", slog.Any("status", status))
					// Continue to send the next status update.
				}
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		newStatus, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
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
}

func (s *FSSReconcileServer) NewFunction(f *model.Function) (*FuncInstance, error) {
	instance := &FuncInstance{
		Name:    f.Name,
		readyCh: make(chan error),
		input:   make(chan string),
		output:  make(chan string),
		status: &pb.FunctionStatus{
			Name:   f.Name,
			Status: pb.FunctionStatus_CREATING,
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
	return instance, nil
}

func (s *FSSReconcileServer) getFunc(name string) (*FuncInstance, error) {
	s.functionsMu.Lock()
	defer s.functionsMu.Unlock()
	instance, ok := s.functions[name]
	if !ok {
		return nil, fmt.Errorf("function not found")
	}
	return instance, nil
}

func (s *FSSReconcileServer) RemoveFunction(name string) {
	s.functionsMu.Lock()
	defer s.functionsMu.Unlock()
	delete(s.functions, name)
}

func (f *FuncInstance) Update(new *pb.FunctionStatus) {
	if f.status.Status != new.Status && f.status.Status == pb.FunctionStatus_CREATING {
		f.readyNotify.Do(func() {
			switch new.Status {
			case pb.FunctionStatus_RUNNING:
				f.readyCh <- nil
			case pb.FunctionStatus_FAILED:
				f.readyCh <- fmt.Errorf("function failed to start")
			default:
				f.readyCh <- fmt.Errorf("function in unknown state")
			}
			close(f.readyCh)
		})
	}
}

func (f *FuncInstance) WaitForReady() <-chan error {
	return f.readyCh
}

func (f *FuncInstance) Call(payload string) string {
	f.input <- payload
	return <-f.output
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
	go func() {
		for {
			select {
			case payload := <-instance.input:
				err := stream.Send(&pb.Event{Payload: payload})
				if err != nil {
					slog.Error("failed to send event", slog.Any("error", err))
					return
				}
			case <-stream.Context().Done():
				return
			}
		}
	}()
	for {
		event, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		instance.output <- event.Payload
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
