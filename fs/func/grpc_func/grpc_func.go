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
}

// FSServiceImpl is the struct that implements the FSServiceServer interface.
type FSServiceImpl struct {
	pb.UnimplementedFSReconcileServer
	// Add any other fields you need here.
	readyCh     chan struct{}
	connected   sync.Once
	reconcile   chan *pb.FunctionStatus
	functions   map[string]*FuncInstance
	functionsMu sync.Mutex
}

func NewFSReconcile() *FSServiceImpl {
	return &FSServiceImpl{
		readyCh:   make(chan struct{}),
		reconcile: make(chan *pb.FunctionStatus, 100),
		functions: make(map[string]*FuncInstance),
	}
}

func (s *FSServiceImpl) WaitForReady() <-chan struct{} {
	return s.readyCh
}

func (s *FSServiceImpl) Reconcile(stream pb.FSReconcile_ReconcileServer) error {
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

func (s *FSServiceImpl) NewFunction(f *model.Function) (*FuncInstance, error) {
	instance := &FuncInstance{
		Name:    f.Name,
		readyCh: make(chan error),
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

func (s *FSServiceImpl) RemoveFunction(name string) {
	s.functionsMu.Lock()
	defer s.functionsMu.Unlock()
	delete(s.functions, name)
}

//
//func (s *FSServiceImpl) RegisterNotifier(n string, f NotifierFunc) {
//	s.notifier_mu.Lock()
//	defer s.notifier_mu.Unlock()
//	if _, ok := s.notifiers[n]; ok {
//		slog.Error("Notifier already exists", slog.Any("name", n))
//		panic("invalid state")
//	}
//	s.notifiers[n] = f
//}
//
//func (s *FSServiceImpl) RemoveNotifier(n string) {
//	s.notifier_mu.Lock()
//	defer s.notifier_mu.Unlock()
//	delete(s.notifiers, n)
//}

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

//func (s *FSServiceImpl) Process(stream pb.FSService_ProcessServer) error {
//	err := stream.Send(&pb.Event{Payload: "hello"})
//	if err != nil {
//		return err
//	}
//
//	event, err := stream.Recv()
//	if err != nil {
//		return err
//	}
//	slog.InfoContext(stream.Context(), "Receive event", slog.Any("payload", event.Payload))
//	return nil
//}

// SetState implements the SetState method of the FSServiceServer interface.
//func (s *FSServiceImpl) SetState(ctx context.Context, req *pb.SetStateRequest) (*pb.Response, error) {
//	return okResponse, nil
//}

func StartGRPCServer(f *FSServiceImpl) error {
	lis, err := net.Listen("tcp", fmt.Sprintf(":%d", 7400))
	if err != nil {
		return err
	}
	s := grpc.NewServer()
	pb.RegisterFSReconcileServer(s, f)
	if err := s.Serve(lis); err != nil {
		return err
	}
	return nil
}
