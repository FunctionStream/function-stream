package rpc

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/clients/gofs/api"
	proto "github.com/functionstream/function-stream/fs/runtime/external/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"io"
	"os"
)

const (
	FSSocketPath  = "FS_SOCKET_PATH"
	DefaultModule = "default"
)

type grpcRPCClient struct {
	runtimeCli proto.RuntimeServiceClient
	funcCli    proto.FunctionServiceClient
}

func NewRPCClient() (FSRPCClient, error) {
	socketPath := os.Getenv(FSSocketPath) // TODO: Support TCP port
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
	return &grpcRPCClient{
		runtimeCli: proto.NewRuntimeServiceClient(conn),
		funcCli:    proto.NewFunctionServiceClient(conn),
	}, nil
}

func (g *grpcRPCClient) OnReceiveEvent(ctx *RPCContext, cb func(api.Event[[]byte]) error) error {
	ch, err := g.Read(ctx)
	if err != nil {
		return err
	}
	for e := range ch {
		if err := cb(e); err != nil {
			return err
		}
	}
	return nil
}

func (g *grpcRPCClient) Read(ctx *RPCContext) (<-chan api.Event[[]byte], error) {
	c, err := g.funcCli.Read(ctx, &proto.ReadRequest{
		Context: &proto.Context{
			FunctionName: ctx.FunctionName,
		},
	})
	if err != nil {
		return nil, err
	}
	ch := make(chan api.Event[[]byte])
	go func() {
		defer close(ch)
		for {
			e, err := c.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				return
			}
			ch <- api.NewEventWithCommit(&e.Payload, func(ctx2 context.Context) error {
				return g.Commit(ctx.warp(ctx2), e.Id)
			})
		}
	}()
	return ch, nil
}

func (g *grpcRPCClient) Write(ctx *RPCContext, e api.Event[[]byte]) error {
	_, err := g.funcCli.Write(ctx, &proto.WriteRequest{
		Context: &proto.Context{
			FunctionName: ctx.FunctionName,
		},
		Payload: *e.Data(),
	})
	return err
}

func (g *grpcRPCClient) Commit(ctx *RPCContext, id string) error {
	_, err := g.funcCli.Commit(ctx, &proto.CommitRequest{
		Context: &proto.Context{
			FunctionName: ctx.FunctionName,
		},
		EventId: id,
	})
	return err
}

func (g *grpcRPCClient) OnEvent(ctx context.Context, serviceId string, modules []string) (<-chan *FunctionEvent, error) {
	c, err := g.runtimeCli.OnEvent(ctx, &proto.OnDeployRequest{
		ServiceId: serviceId,
		Modules:   modules,
	})
	if err != nil {
		return nil, err
	}
	ch := make(chan *FunctionEvent)
	go func() {
		defer close(ch)
		for {
			e, err := c.Recv()
			if err == io.EOF {
				return
			}
			if err != nil {
				return
			}
			fe := &FunctionEvent{}
			switch e.GetType() {
			case proto.FunctionEventType_DEPLOY:
				fe.Type = FunctionEventType_DEPLOY
				f := e.GetPayload().(*proto.FunctionEvent_Function).Function
				fe.Function = &Function{
					Name:    f.Name,
					Package: f.Package,
					Module:  f.Module,
					Config:  f.Config,
				}
			case proto.FunctionEventType_DELETE:
				fe.Type = FunctionEventType_DELETE
				fe.FunctionName = &e.GetPayload().(*proto.FunctionEvent_FunctionName).FunctionName
			}
			ch <- fe
		}
	}()
	return ch, nil
}

func (g *grpcRPCClient) RegisterSchema(ctx context.Context, schema string) error {
	return nil
}

func (g *grpcRPCClient) GetState(ctx *RPCContext, key string) ([]byte, error) {
	//TODO implement me
	panic("implement me")
}

func (g *grpcRPCClient) PutState(ctx *RPCContext, key string, value []byte) error {
	//TODO implement me
	panic("implement me")
}

func (g *grpcRPCClient) ListStates(ctx *RPCContext, path string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
