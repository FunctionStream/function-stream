package external

import (
	"bytes"
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/event"
	proto "github.com/functionstream/function-stream/fs/runtime/external/proto"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"io"
	"net"
	"sync"
)

//go:generate protoc --go_out=paths=source_relative:. --go-grpc_out=paths=source_relative:. proto/fs.proto

type Config struct {
	Listener net.Listener
	Logger   *zap.Logger
}

type adapterImpl struct {
	proto.RuntimeServiceServer
	proto.FunctionServiceServer
	log *zap.Logger

	instances map[string]api.Instance

	servicesMu sync.Mutex
	services   map[string]service // module -> service
}

type service struct {
	eventCh chan *proto.FunctionEvent
}

func (a *adapterImpl) OnEvent(request *proto.OnDeployRequest, g grpc.ServerStreamingServer[proto.FunctionEvent]) error {
	svc := service{
		eventCh: make(chan *proto.FunctionEvent),
	}
	log := a.log.Named(request.ServiceId)
	log.Info("Setting up function service")
	a.servicesMu.Lock()
	for _, m := range request.Modules {
		if _, exist := a.services[m]; exist {
			log.Error("module already exists", zap.String("module", m))
			return status.Error(codes.AlreadyExists, fmt.Sprintf("module %s already exists", m))
		}
	}
	for _, m := range request.Modules {
		a.services[m] = svc
	}
	a.servicesMu.Unlock()
	log.Info("Function service setup complete")
	for {
		select {
		case <-g.Context().Done():
			a.servicesMu.Lock()
			for _, m := range request.Modules {
				delete(a.services, m)
			}
			a.servicesMu.Unlock()
			close(svc.eventCh)
			log.Info("Function service stopped")
			return nil
		case e := <-svc.eventCh:
			log.Info("Sending function event", zap.String("type", e.Type.String()))
			err := g.Send(e)
			if err != nil {
				log.Error("failed to send function event", zap.Error(err))
				return status.Error(codes.Internal, "failed to send function event")
			}
		}
	}
}

func (a *adapterImpl) DeployFunction(ctx context.Context, instance api.Instance) error {
	// TODO: Support interceptor to process the package
	e := &proto.FunctionEvent{
		Type: proto.FunctionEventType_DEPLOY,
		Payload: &proto.FunctionEvent_Function{
			Function: &proto.Function{
				Name:    instance.Function().Name,
				Package: instance.Function().Package,
				Module:  instance.Function().Module,
				Config:  instance.Function().Config,
			},
		},
	}
	a.log.Info("Deploying function", zap.String("name", instance.Function().Name))
	a.servicesMu.Lock()
	a.instances[instance.Function().Name] = instance
	svc, ok := a.services[instance.Function().Module]
	if !ok {
		return fmt.Errorf("module %s not found", instance.Function().Module)
	}
	a.servicesMu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case svc.eventCh <- e:
		return nil
	}
}

func (a *adapterImpl) DeleteFunction(ctx context.Context, name string) error {
	e := &proto.FunctionEvent{
		Type: proto.FunctionEventType_DELETE,
		Payload: &proto.FunctionEvent_FunctionName{
			FunctionName: name,
		},
	}
	a.log.Info("Deleting function", zap.String("name", name))
	for _, svc := range a.services {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case svc.eventCh <- e:
		}
	}
	return nil
}

func (a *adapterImpl) getInstance(name string) (api.Instance, error) {
	ins, ok := a.instances[name]
	if !ok {
		msg := fmt.Sprintf("function runtime %s not found", name)
		return nil, status.Error(codes.Unavailable, msg)
	}
	return ins, nil
}

func (a *adapterImpl) Read(request *proto.ReadRequest, g grpc.ServerStreamingServer[proto.Event]) error {
	ins, err := a.getInstance(request.GetContext().GetFunctionName())
	if err != nil {
		return err
	}
	readCh, err := ins.EventStorage().Read(g.Context(), ins.Function().Sources)
	if err != nil {
		return err
	}
	l := a.log.With(zap.String("function", ins.Function().Name))
	for {
		select {
		case <-g.Context().Done():
			return nil
		case e, ok := <-readCh:
			if !ok {
				return nil
			}
			p, err := io.ReadAll(e.Payload())
			if err != nil {
				return fmt.Errorf("failed to read event payload: %w", err)
			}
			protoE := &proto.Event{
				Id:         e.ID(),
				SchemaId:   e.SchemaID(),
				Payload:    p,
				Properties: e.Properties(),
			}
			if l.Core().Enabled(zap.DebugLevel) {
				l.Debug("Sending event", zap.Any("event", protoE))
			}
			err = g.Send(protoE)
			if err != nil {
				return err
			}
		}
	}
}

func (a *adapterImpl) Write(ctx context.Context, request *proto.WriteRequest) (*proto.WriteResponse, error) {
	ins, err := a.getInstance(request.GetContext().GetFunctionName())
	if err != nil {
		return nil, err
	}
	e := event.NewRawEvent("", bytes.NewReader(request.GetPayload()))
	err = ins.EventStorage().Write(ctx, e, ins.Function().Sink)
	if err != nil {
		return nil, err
	}
	l := a.log.With(zap.String("function", ins.Function().Name))
	if l.Core().Enabled(zap.DebugLevel) {
		l.Debug("Write event to storage", zap.String("event_id", e.ID()))
	}
	select {
	case <-e.OnCommit():
		return &proto.WriteResponse{}, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (a *adapterImpl) Commit(ctx context.Context, request *proto.CommitRequest) (*proto.CommitResponse, error) {
	ins, err := a.getInstance(request.GetContext().GetFunctionName())
	if err != nil {
		return nil, err
	}
	err = ins.EventStorage().Commit(ctx, request.GetEventId())
	if err != nil {
		return nil, err
	}
	return &proto.CommitResponse{}, nil
}

func (a *adapterImpl) PutState(ctx context.Context, request *proto.PutStateRequest) (*proto.PutStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *adapterImpl) GetState(ctx context.Context, request *proto.GetStateRequest) (*proto.GetStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *adapterImpl) ListStates(ctx context.Context, request *proto.ListStatesRequest) (*proto.ListStatesResponse, error) {
	//TODO implement me
	panic("implement me")
}

func (a *adapterImpl) DeleteState(ctx context.Context, request *proto.DeleteStateRequest) (*proto.DeleteStateResponse, error) {
	//TODO implement me
	panic("implement me")
}

func NewExternalAdapter(ctx context.Context, name string, c *Config) (api.RuntimeAdapter, error) {
	l := c.Logger.Named("external-adapter").Named(name)
	s := grpc.NewServer()
	a := &adapterImpl{
		log:       l,
		instances: make(map[string]api.Instance),
		services:  make(map[string]service),
	}
	proto.RegisterRuntimeServiceServer(s, a)
	proto.RegisterFunctionServiceServer(s, a)
	// TODO: Setup health service
	go func() {
		l.Info("starting external adapter")
		if err := s.Serve(c.Listener); err != nil {
			l.Error("failed to start external adapter", zap.Error(err))
			panic(err)
		}
	}()
	go func() {
		<-ctx.Done()
		l.Info("stopping external adapter")
		s.Stop()
	}()
	return a, nil
}
