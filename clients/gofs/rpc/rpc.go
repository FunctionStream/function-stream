package rpc

import (
	"context"
	"github.com/functionstream/function-stream/clients/gofs/api"
)

type RPCContext struct {
	context.Context
	FunctionName string
}

func (r *RPCContext) warp(ctx context.Context) *RPCContext {
	return &RPCContext{
		Context:      ctx,
		FunctionName: r.FunctionName,
	}
}

type FunctionEventType int32

const (
	FunctionEventType_DEPLOY FunctionEventType = 0
	FunctionEventType_DELETE FunctionEventType = 1
)

type Function struct {
	Name    string
	Package string
	Module  string
	Config  map[string]string
}

type FunctionEvent struct {
	Type FunctionEventType

	// Payload:
	Function     *Function
	FunctionName *string
}

type FSRPCClient interface {
	Read(ctx *RPCContext) (<-chan api.Event[[]byte], error)
	OnReceiveEvent(ctx *RPCContext, cb func(api.Event[[]byte]) error) error
	Write(ctx *RPCContext, e api.Event[[]byte]) error
	Commit(ctx *RPCContext, id string) error
	OnEvent(ctx context.Context, serviceId string, modules []string) (<-chan *FunctionEvent, error)
	RegisterSchema(ctx context.Context, schema string) error
	GetState(ctx *RPCContext, key string) ([]byte, error)
	PutState(ctx *RPCContext, key string, value []byte) error
	ListStates(ctx *RPCContext, path string) ([]string, error)
}
