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
	"fmt"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/lifecycle"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/fs/runtime/grpc/proto"
	"golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
)

type GRPCFuncRuntime struct {
	*lifecycle.Lifecycle
	Name       string
	instance   api.FunctionInstance
	ctx        context.Context
	status     *proto.FunctionStatus
	readyOnce  sync.Once
	readyCh    chan error
	input      chan contube.Record
	output     chan contube.Record
	stopFunc   func()
	processing atomic.Bool
	log        *slog.Logger
}

type Status int32

const (
	NotReady Status = iota
	Ready
)

// FSSReconcileServer is the struct that implements the FSServiceServer interface.
type FSSReconcileServer struct {
	proto.UnimplementedFSReconcileServer
	ctx         context.Context
	readyCh     chan struct{}
	connected   sync.Once
	reconcile   chan *proto.FunctionStatus
	functions   map[string]*GRPCFuncRuntime
	functionsMu sync.Mutex
	status      int32
	log         *slog.Logger
}

func NewFSReconcile(ctx context.Context) *FSSReconcileServer {
	return &FSSReconcileServer{
		ctx:       ctx,
		readyCh:   make(chan struct{}),
		reconcile: make(chan *proto.FunctionStatus, 100),
		functions: make(map[string]*GRPCFuncRuntime),
		log:       slog.With(slog.String("component", "grpc-reconcile-server")),
	}
}

func (s *FSSReconcileServer) WaitForReady() <-chan struct{} {
	return s.readyCh
}

func (s *FSSReconcileServer) Reconcile(_ *proto.ConnectRequest, stream proto.FSReconcile_ReconcileServer) error {
	s.connected.Do(func() {
		close(s.readyCh)
	})
	if Status(atomic.LoadInt32(&s.status)) == Ready {
		return fmt.Errorf("there is already a reconcile stream")
	}
	s.log.InfoContext(s.ctx, "reconcile client connected")
	defer func() {
		atomic.StoreInt32(&s.status, int32(NotReady))
		s.log.InfoContext(s.ctx, "grpc reconcile stream closed")
	}()
	// Sync all exiting function status to the newly connected reconcile client
	s.functionsMu.Lock()
	statusList := make([]*proto.FunctionStatus, 0, len(s.functions))
	for _, v := range s.functions {
		statusList = append(statusList, v.status)
	}
	s.functionsMu.Unlock()
	for _, v := range statusList {
		err := stream.Send(v)
		if err != nil {
			s.log.ErrorContext(stream.Context(), "failed to send status update", slog.Any("status", v))
			// Continue to send the next status update.
		}
	}
	for {
		select {
		case status := <-s.reconcile:
			s.log.DebugContext(s.ctx, "sending status update", slog.Any("status", status))
			err := stream.Send(status)
			if err != nil {
				s.log.ErrorContext(stream.Context(), "failed to send status update", slog.Any("status", status))
				// Continue to send the next status update.
			}
		case <-stream.Context().Done():
			return nil
		case <-s.ctx.Done():
			return nil
		}
	}

}

func (s *FSSReconcileServer) UpdateStatus(_ context.Context, newStatus *proto.FunctionStatus) (*proto.Response, error) {
	s.log.DebugContext(s.ctx, "received status update", slog.Any("status", newStatus))
	s.functionsMu.Lock()
	instance, ok := s.functions[newStatus.Name]
	if !ok {
		s.functionsMu.Unlock()
		s.log.ErrorContext(s.ctx, "receive non-exist function status update", slog.Any("name", newStatus.Name))
		return &proto.Response{
			Status:  proto.Response_ERROR,
			Message: common.OptionalStr("function not found"),
		}, nil
	}
	s.functionsMu.Unlock()
	instance.Update(newStatus)
	return &proto.Response{
		Status: proto.Response_OK,
	}, nil
}

func (s *FSSReconcileServer) NewFunctionRuntime(instance api.FunctionInstance) (api.FunctionRuntime, error) {
	name := instance.Definition().Name
	log := instance.Logger().With(
		slog.String("component", "grpc-runtime"),
	)
	runtime := &GRPCFuncRuntime{
		Lifecycle: lifecycle.NewLifecycle(lifecycle.WithParent(instance.GetLifecycle()),
			lifecycle.WithCloseFunc(func() error {
				s.removeFunction(name)
				return nil
			})),
		Name:     name,
		instance: instance,
		readyCh:  make(chan error),
		input:    make(chan contube.Record),
		output:   make(chan contube.Record),
		status: &proto.FunctionStatus{
			Name:   name,
			Status: proto.FunctionStatus_CREATING,
		},
		ctx: instance.Context(),
		log: log,
	}
	{
		s.functionsMu.Lock()
		defer s.functionsMu.Unlock()
		if _, ok := s.functions[name]; ok {
			return nil, fmt.Errorf("function already exists")
		}
		s.functions[name] = runtime
	}
	s.reconcile <- runtime.status
	log.InfoContext(runtime.ctx, "Creating GRPC function runtime")
	return runtime, nil
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
	s.log.InfoContext(instance.ctx, "Removing function", slog.Any("name", name))
	delete(s.functions, name)
}

func isFinalStatus(status proto.FunctionStatus_Status) bool {
	return status == proto.FunctionStatus_FAILED || status == proto.FunctionStatus_DELETED
}

func (f *GRPCFuncRuntime) Update(new *proto.FunctionStatus) {
	if f.status.Status == proto.FunctionStatus_CREATING && isFinalStatus(new.Status) {
		f.readyCh <- fmt.Errorf("function failed to start")
	}
	if f.status.Status != new.Status {
		f.log.InfoContext(f.ctx, "Function status update", slog.Any("new_status", new.Status), slog.Any("old_status", f.status.Status))
	}
	f.status = new
}

func (f *GRPCFuncRuntime) WaitForReady() <-chan error {
	return f.readyCh
}

// Stop stops the function runtime and remove it
// It is different from the ctx.Cancel. It will make sure the runtime has been deleted after this method returns.
func (f *GRPCFuncRuntime) Stop() {
	f.log.InfoContext(f.ctx, "Stopping function runtime")
	f.stopFunc()
}

func (f *GRPCFuncRuntime) Call(event contube.Record) (contube.Record, error) {
	f.input <- event
	out := <-f.output
	f.processing.Store(false)
	return out, nil
}

type FunctionServerImpl struct {
	proto.UnimplementedFunctionServer
	reconcileSvr *FSSReconcileServer
}

func NewFunctionServerImpl(s *FSSReconcileServer) *FunctionServerImpl {
	return &FunctionServerImpl{
		reconcileSvr: s,
	}
}

func (f *FunctionServerImpl) Process(req *proto.FunctionProcessRequest, stream proto.Function_ProcessServer) error {
	runtime, err := f.reconcileSvr.getFunc(req.Name)
	if err != nil {
		return err
	}
	log := runtime.log
	log.InfoContext(stream.Context(), "Start processing events using GRPC")
	runtime.readyOnce.Do(func() {
		runtime.readyCh <- err
	})
	errCh := make(chan error)

	defer func() {
		if runtime.processing.Load() {
			runtime.output <- nil
			runtime.processing.Store(false)
		}
	}()

	logCounter := common.LogCounter()
	for {
		select {
		case event := <-runtime.input:
			log.DebugContext(stream.Context(), "sending event", slog.Any("count", logCounter))
			runtime.processing.Store(true)
			err := stream.Send(&proto.Event{Payload: string(event.GetPayload())}) // TODO: Change payload type to bytes
			if err != nil {
				log.Error("failed to send event", slog.Any("error", err))
				return err
			}
		case <-stream.Context().Done():
			return nil
		case <-runtime.ctx.Done():
			return nil
		case e := <-errCh:
			return e
		}
	}
}

func (f *FunctionServerImpl) getFunctionRuntime(ctx context.Context) (*GRPCFuncRuntime, error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, fmt.Errorf("failed to get metadata")
	}
	if _, ok := md["name"]; !ok || len(md["name"]) == 0 {
		return nil, fmt.Errorf("the metadata doesn't contain the function name")
	}
	return f.reconcileSvr.getFunc(md["name"][0])
}

func (f *FunctionServerImpl) Output(ctx context.Context, e *proto.Event) (*proto.Response, error) {
	runtime, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	runtime.log.DebugContext(ctx, "received event")
	runtime.output <- contube.NewRecordImpl([]byte(e.Payload), func() {})
	return &proto.Response{
		Status: proto.Response_OK,
	}, nil
}

func (f *FunctionServerImpl) PutState(ctx context.Context, req *proto.PutStateRequest) (*proto.Response, error) {
	runtime, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	runtime.log.DebugContext(ctx, "put state")
	err = runtime.instance.FunctionContext().PutState(req.Key, req.Value)
	if err != nil {
		return nil, err
	}
	return &proto.Response{
		Status: proto.Response_OK,
	}, nil
}

func (f *FunctionServerImpl) GetState(ctx context.Context, req *proto.GetStateRequest) (*proto.GetStateResponse, error) {
	runtime, err := f.getFunctionRuntime(ctx)
	if err != nil {
		return nil, err
	}
	runtime.log.DebugContext(ctx, "get state")
	v, err := runtime.instance.FunctionContext().GetState(req.Key)
	if err != nil {
		return nil, err
	}
	return &proto.GetStateResponse{
		Value: v,
	}, nil
}

func StartGRPCServer(f *FSSReconcileServer, addr string) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	s := grpc.NewServer()
	proto.RegisterFSReconcileServer(s, f)
	proto.RegisterFunctionServer(s, NewFunctionServerImpl(f))
	go func() {
		if err := s.Serve(lis); err != nil {
			slog.Error("failed to serve", slog.Any("error", err))
		}
	}()
	return s, nil
}
