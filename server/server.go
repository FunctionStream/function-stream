package server

import (
	"context"
	"fmt"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/api"
	memory2 "github.com/functionstream/function-stream/fs/eventstorage/memory"
	"github.com/functionstream/function-stream/fs/packagestorage/memory"
	"github.com/functionstream/function-stream/fs/runtime/external"
	memory3 "github.com/functionstream/function-stream/fs/statestore/memory"
	"github.com/functionstream/function-stream/server/rest"
	"go.uber.org/zap"
	"net"
)

type ServerOption struct {
	PackageStorageFactory func(ctx context.Context, config *ComponentConfig) (api.PackageStorage, error)
	EventStorageFactory   func(ctx context.Context, config *ComponentConfig) (api.EventStorage, error)
	StateStoreFactory     func(ctx context.Context, config *ComponentConfig) (api.StateStore, error)
	RuntimeFactory        func(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error)

	Logger *zap.Logger
}

func (o *ServerOption) BuiltinPackageLoaderFactory(_ context.Context, config *ComponentConfig) (api.PackageStorage, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory.NewMemoryPackageStorage(o.Logger), nil
	}
	return nil, fmt.Errorf("unknown package loader type: %s", config.Type)
}

func (o *ServerOption) GetPackageStorageFactory() func(ctx context.Context, config *ComponentConfig) (api.PackageStorage, error) {
	if o.PackageStorageFactory != nil {
		return o.PackageStorageFactory
	}
	return o.BuiltinPackageLoaderFactory
}

func (o *ServerOption) BuiltinEventStorageFactory(ctx context.Context, config *ComponentConfig) (api.EventStorage, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory2.NewMemEs(ctx, o.Logger), nil
	}
	return nil, fmt.Errorf("unknown event storage type: %s", config.Type)
}

func (o *ServerOption) GetEventStorageFactory() func(ctx context.Context, config *ComponentConfig) (api.EventStorage, error) {
	if o.EventStorageFactory != nil {
		return o.EventStorageFactory
	}
	return o.BuiltinEventStorageFactory
}

func (o *ServerOption) BuiltinStateStoreFactory(_ context.Context, config *ComponentConfig) (api.StateStore, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory3.NewMemoryStateStore(), nil
	}
	return nil, fmt.Errorf("unknown state store type: %s", config.Type)
}

func (o *ServerOption) GetStateStoreFactory() func(ctx context.Context, config *ComponentConfig) (api.StateStore, error) {
	if o.StateStoreFactory != nil {
		return o.StateStoreFactory
	}
	return o.BuiltinStateStoreFactory
}

func (o *ServerOption) BuiltinRuntimeFactory(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error) {
	switch config.Type {
	case "":
	case "external":

		c := &struct {
			ListenAddress string `json:"address"`
		}{}
		if err := config.Config.ToConfigStruct(c); err != nil {
			return nil, err
		}
		ln, err := net.Listen("tcp", c.ListenAddress)
		if err != nil {
			return nil, fmt.Errorf("failed to listen: %w", err)
		}
		eaCfg := &external.Config{
			Listener: ln,
			Logger:   o.Logger,
		}
		return external.NewExternalAdapter(ctx, config.Type, eaCfg)
	}
	return nil, fmt.Errorf("unknown runtime type: %s", config.Type)
}

func (o *ServerOption) GetRuntimeFactory() func(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error) {
	if o.RuntimeFactory != nil {
		return o.RuntimeFactory
	}
	return o.BuiltinRuntimeFactory
}

type Server struct {
	handler *rest.Handler
	log     *zap.Logger
}

func NewServer(svrCtx context.Context, config *Config, opt *ServerOption) (*Server, error) {
	log := opt.Logger
	mgrCfg := &fs.ManagerConfig{}

	if opt.Logger == nil {
		opt.Logger = zap.NewNop()
	}

	ln, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return nil, fmt.Errorf("failed to listen: %w", err)
	}

	pkgStorage, err := opt.GetPackageStorageFactory()(svrCtx, &config.PackageLoader)
	if err != nil {
		return nil, fmt.Errorf("failed to create package storage: %w", err)
	}
	mgrCfg.PackageLoader = pkgStorage
	log.Info("Package storage loaded", zap.String("type", config.PackageLoader.Type))

	eventStorage, err := opt.GetEventStorageFactory()(svrCtx, &config.EventStorage)
	if err != nil {
		return nil, fmt.Errorf("failed to create event storage: %w", err)
	}
	mgrCfg.EventStorage = eventStorage
	log.Info("Event storage loaded", zap.String("type", config.EventStorage.Type))

	stateStore, err := opt.GetStateStoreFactory()(svrCtx, &config.StateStore)
	if err != nil {
		return nil, fmt.Errorf("failed to create state store: %w", err)
	}
	mgrCfg.StateStore = stateStore
	log.Info("State store loaded", zap.String("type", config.StateStore.Type))

	runtimeMap := make(map[string]api.RuntimeAdapter)
	for _, rtCfg := range config.Runtimes {
		runtime, err := opt.GetRuntimeFactory()(svrCtx, &rtCfg)
		if err != nil {
			return nil, fmt.Errorf("failed to create runtime: %w", err)
		}
		runtimeMap[rtCfg.Type] = runtime
		log.Info("Runtime loaded", zap.String("type", rtCfg.Type))
	}
	mgrCfg.RuntimeMap = runtimeMap

	mgr, err := fs.NewManager(*mgrCfg, log.Named("manager"))
	if err != nil {
		return nil, fmt.Errorf("failed to create manager: %w", err)
	}

	handler := &rest.Handler{
		Manager:        mgr,
		PackageStorage: pkgStorage,
		EventStorage:   eventStorage,
		EnableTls:      config.EnableTLS,
		TlsCertFile:    config.TLSCertFile,
		TlsKeyFile:     config.TLSKeyFile,
		HttpListener:   ln,
		Log:            opt.Logger,
	}

	return &Server{
		handler: handler,
		log:     opt.Logger,
	}, nil
}

func (s *Server) Run(ctx context.Context) error {
	go func() {
		<-ctx.Done()
		if err := s.handler.Stop(); err != nil {
			s.log.Error("Failed to stop REST handler", zap.Error(err))
		}

	}()
	return s.handler.Run()
}

func (s *Server) WaitForReady(ctx context.Context) <-chan struct{} {
	return s.handler.WaitForReady(ctx)
}
