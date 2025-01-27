package gofs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/functionstream/function-stream/clients/gofs/api"
	"github.com/functionstream/function-stream/clients/gofs/rpc"
	"github.com/wirelessr/avroschema"
	"go.uber.org/zap"
	"log/slog"
	"sync"
)

const (
	StateInit int32 = iota
	StateRunning
)

var (
	ErrRegisterModuleDuringRunning = fmt.Errorf("cannot register module during running")
	ErrAlreadyRunning              = fmt.Errorf("already running")
)

type FSClient interface {
	error
	Register(module string, wrapperFact InstanceWrapperFactory) FSClient
	Run(ctx context.Context) error
}

type fsClient struct {
	rpc        rpc.FSRPCClient
	modules    map[string]InstanceWrapperFactory
	state      int32
	registerMu sync.Mutex
	err        error

	instances map[string]*InstanceWrapper
}

func (c *fsClient) Error() string {
	return c.err.Error()
}

func NewFSClient() FSClient {
	return &fsClient{
		modules:   make(map[string]InstanceWrapperFactory),
		state:     StateInit,
		instances: make(map[string]*InstanceWrapper),
	}
}

type InstanceWrapper struct {
	*fsClient
	ctx         *functionContextImpl
	rpcCtx      *rpc.RPCContext
	executeFunc func(api.FunctionContext) error
	initFunc    func(api.FunctionContext) error
	registerErr error
}

func (m *InstanceWrapper) AddInitFunc(initFunc func(api.FunctionContext) error) *InstanceWrapper {
	parentInit := m.initFunc
	if parentInit != nil {
		m.initFunc = func(ctx api.FunctionContext) error {
			err := parentInit(ctx)
			if err != nil {
				return err
			}
			return initFunc(ctx)
		}
	} else {
		m.initFunc = initFunc
	}
	return m
}

type InstanceWrapperFactory func() *InstanceWrapper

func (c *fsClient) Register(module string, wrapperFact InstanceWrapperFactory) FSClient {
	if c.err != nil {
		return c
	}
	c.registerMu.Lock()
	defer c.registerMu.Unlock()
	if c.state == StateRunning {
		c.err = ErrRegisterModuleDuringRunning
		return c
	}
	c.modules[module] = wrapperFact
	return c
}

func WithFunction[I any, O any](functionFactory func() api.Function[I, O]) InstanceWrapperFactory {
	return func() *InstanceWrapper {
		m := &InstanceWrapper{}
		function := functionFactory()
		m.initFunc = func(ctx api.FunctionContext) error {
			outputSchema, err := avroschema.Reflect(new(O))
			if err != nil {
				return err
			}
			err = m.rpc.RegisterSchema(ctx, outputSchema)
			if err != nil {
				return fmt.Errorf("failed to register schema: %w", err)
			}
			return function.Init(ctx)
		}
		m.executeFunc = func(ctx api.FunctionContext) error {
			return m.rpc.OnReceiveEvent(m.rpcCtx, func(event api.Event[[]byte]) error {
				input := new(I)
				err := json.Unmarshal(*event.Data(), input)
				if err != nil {
					return fmt.Errorf("failed to parse JSON: %w", err)
				}
				output, err := function.Handle(ctx, api.NewEventWithCommit(input, event.Commit))
				if err != nil {
					return err
				}
				outputPayload, err := json.Marshal(output.Data())
				if err != nil {
					return fmt.Errorf("failed to marshal JSON: %w", err)
				}
				return ctx.Write(ctx, api.NewEventWithCommit(&outputPayload, func(ctx context.Context) error {
					return errors.Join(event.Commit(ctx), output.Commit(ctx))
				}))
			})
		}
		return m
	}
}

func WithSource[O any](sourceFactory func() api.Source[O]) InstanceWrapperFactory {
	return func() *InstanceWrapper {
		m := &InstanceWrapper{}
		source := sourceFactory()
		emit := func(ctx context.Context, event api.Event[O]) error {
			outputPayload, _ := json.Marshal(event.Data())
			return m.ctx.Write(ctx, api.NewEventWithCommit(&outputPayload, event.Commit))
		}
		m.initFunc = func(ctx api.FunctionContext) error {
			outputSchema, err := avroschema.Reflect(new(O))
			if err != nil {
				return err
			}
			err = m.rpc.RegisterSchema(ctx, outputSchema)
			if err != nil {
				return fmt.Errorf("failed to register schema: %w", err)
			}
			return source.Init(ctx)
		}
		m.executeFunc = func(ctx api.FunctionContext) error {
			return source.Handle(ctx, emit)
		}
		return m
	}
}

func WithSink[I any](sinkFactory func() api.Sink[I]) InstanceWrapperFactory {
	return func() *InstanceWrapper {
		m := &InstanceWrapper{}
		sink := sinkFactory()
		m.initFunc = func(ctx api.FunctionContext) error {
			inputSchema, err := avroschema.Reflect(new(I))
			if err != nil {
				return err
			}
			err = m.rpc.RegisterSchema(ctx, inputSchema)
			if err != nil {
				return fmt.Errorf("failed to register schema: %w", err)
			}
			return sink.Init(ctx)
		}
		m.executeFunc = func(ctx api.FunctionContext) error {
			return m.rpc.OnReceiveEvent(m.rpcCtx, func(event api.Event[[]byte]) error {
				input := new(I)
				err := json.Unmarshal(*event.Data(), input)
				if err != nil {
					return fmt.Errorf("failed to parse JSON: %w", err)
				}
				return sink.Handle(ctx, api.NewEventWithCommit(input, event.Commit))
			})
		}
		return m
	}
}

func WithCustom(customFactory func() api.Custom) InstanceWrapperFactory {
	return func() *InstanceWrapper {
		custom := customFactory()
		return &InstanceWrapper{
			initFunc: func(ctx api.FunctionContext) error {
				return custom.Init(ctx)
			},
			executeFunc: func(ctx api.FunctionContext) error {
				return custom.Handle(ctx)
			},
		}
	}
}

type functionContextImpl struct {
	context.Context
	cancelFunc context.CancelFunc

	c      *fsClient
	name   string
	config map[string]string
}

func (c *functionContextImpl) warpContext(parent context.Context) *rpc.RPCContext {
	return &rpc.RPCContext{
		Context:      parent,
		FunctionName: c.name,
	}
}

func (c *functionContextImpl) GetState(ctx context.Context, key string) ([]byte, error) {
	return c.c.rpc.GetState(c.warpContext(ctx), key)
}

func (c *functionContextImpl) PutState(ctx context.Context, key string, value []byte) error {
	return c.c.rpc.PutState(c.warpContext(ctx), key, value)
}

func (c *functionContextImpl) Write(ctx context.Context, rawEvent api.Event[[]byte]) error {
	return c.c.rpc.Write(c.warpContext(ctx), rawEvent)
}

func (c *functionContextImpl) Read(ctx context.Context) (<-chan api.Event[[]byte], error) {
	return c.c.rpc.Read(c.warpContext(ctx))
}

func (c *functionContextImpl) GetConfig() map[string]string {
	return c.config
}

func (c *functionContextImpl) Close() {
	c.cancelFunc()
}

func (c *fsClient) Run(ctx context.Context) error {
	if c.err != nil {
		return c.err
	}
	c.registerMu.Lock()
	if c.state == StateRunning {
		c.registerMu.Unlock()
		return ErrAlreadyRunning
	}
	c.state = StateRunning
	c.registerMu.Unlock()

	rpcCli, err := rpc.NewRPCClient()
	if err != nil {
		return err
	}
	c.rpc = rpcCli

	modList := make([]string, 0, len(c.modules))
	for k := range c.modules {
		modList = append(modList, k)
	}

	eventChan, err := rpcCli.OnEvent(ctx, "", modList)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case e := <-eventChan:
			switch e.Type {
			case rpc.FunctionEventType_DEPLOY:
				mf, ok := c.modules[e.Function.Module]
				if !ok {
					return fmt.Errorf("module %s not found", e.Function.Module)
				}
				m := mf()
				fc, cancel := context.WithCancel(ctx)
				funcCtx := &functionContextImpl{
					Context:    fc,
					cancelFunc: cancel,
					c:          c,
					name:       e.Function.Name,
				}
				m.fsClient = c
				m.ctx = funcCtx
				m.rpcCtx = funcCtx.warpContext(funcCtx)
				c.instances[e.Function.Name] = m
				go func() {
					err := m.initFunc(funcCtx)
					if err != nil {
						slog.Error("failed to init function", zap.Error(err)) // TODO: We need to figure out how to print log in the function
						return
					}
					err = m.executeFunc(funcCtx)
					if err != nil && !errors.Is(err, context.Canceled) {
						slog.Error("failed to execute function", zap.Error(err)) // TODO: We need to figure out how to print log in the function
						return
					}
				}()
			case rpc.FunctionEventType_DELETE:
				m, ok := c.instances[*e.FunctionName]
				if !ok {
					slog.Warn("delete failed. function not found", slog.String("name", *e.FunctionName))
				} else {
					m.ctx.Close()
					delete(c.instances, *e.FunctionName)
				}
			}
		}
	}
}
