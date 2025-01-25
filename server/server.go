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

type NewServerOption struct {
	PackageStorageFactory func(ctx context.Context, config *ComponentConfig) (api.PackageStorage, error)
	EventStorageFactory   func(ctx context.Context, config *ComponentConfig) (api.EventStorage, error)
	StateStoreFactory     func(ctx context.Context, config *ComponentConfig) (api.StateStore, error)
	RuntimeFactory        func(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error)

	log *zap.Logger
}

func (o *NewServerOption) BuiltinPackageLoaderFactory(_ context.Context, config *ComponentConfig) (api.PackageStorage, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory.NewMemoryPackageStorage(), nil
	}
	return nil, fmt.Errorf("unknown package loader type: %s", config.Type)
}

func (o *NewServerOption) GetPackageStorageFactory() func(ctx context.Context, config *ComponentConfig) (api.PackageStorage, error) {
	if o.PackageStorageFactory != nil {
		return o.PackageStorageFactory
	}
	return o.BuiltinPackageLoaderFactory
}

func (o *NewServerOption) BuiltinEventStorageFactory(_ context.Context, config *ComponentConfig) (api.EventStorage, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory2.NewMemoryEventStorage(o.log), nil
	}
	return nil, fmt.Errorf("unknown event storage type: %s", config.Type)
}

func (o *NewServerOption) GetEventStorageFactory() func(ctx context.Context, config *ComponentConfig) (api.EventStorage, error) {
	if o.EventStorageFactory != nil {
		return o.EventStorageFactory
	}
	return o.BuiltinEventStorageFactory
}

func (o *NewServerOption) BuiltinStateStoreFactory(_ context.Context, config *ComponentConfig) (api.StateStore, error) {
	switch config.Type {
	case "":
	case "memory":
		return memory3.NewMemoryStateStore(), nil
	}
	return nil, fmt.Errorf("unknown state store type: %s", config.Type)
}

func (o *NewServerOption) GetStateStoreFactory() func(ctx context.Context, config *ComponentConfig) (api.StateStore, error) {
	if o.StateStoreFactory != nil {
		return o.StateStoreFactory
	}
	return o.BuiltinStateStoreFactory
}

func (o *NewServerOption) BuiltinRuntimeFactory(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error) {
	switch config.Type {
	case "":
	case "external":
		c := &external.Config{}
		if err := config.Config.ToConfigStruct(c); err != nil {
			return nil, err
		}
		return external.NewExternalAdapter(ctx, config.Type, c)
	}
	return nil, fmt.Errorf("unknown runtime type: %s", config.Type)
}

func (o *NewServerOption) GetRuntimeFactory() func(ctx context.Context, config *ComponentConfig) (api.RuntimeAdapter, error) {
	if o.RuntimeFactory != nil {
		return o.RuntimeFactory
	}
	return o.BuiltinRuntimeFactory
}

func RunServer(ctx context.Context, config *Config, opt *NewServerOption) error {
	mgrCfg := &fs.ManagerConfig{}

	if opt.log == nil {
		opt.log = zap.NewNop()
	}

	ln, err := net.Listen("tcp", config.ListenAddress)
	if err != nil {
		return fmt.Errorf("failed to listen: %w", err)
	}

	pkgStorage, err := opt.GetPackageStorageFactory()(ctx, &config.PackageLoader)
	if err != nil {
		return fmt.Errorf("failed to create package storage: %w", err)
	}
	mgrCfg.PackageLoader = pkgStorage

	eventStorage, err := opt.GetEventStorageFactory()(ctx, &config.EventStorage)
	if err != nil {
		return fmt.Errorf("failed to create event storage: %w", err)
	}
	mgrCfg.EventStorage = eventStorage

	stateStore, err := opt.GetStateStoreFactory()(ctx, &config.StateStore)
	if err != nil {
		return fmt.Errorf("failed to create state store: %w", err)
	}
	mgrCfg.StateStore = stateStore

	runtimeMap := make(map[string]api.RuntimeAdapter)
	for _, rtCfg := range config.Runtimes {
		runtime, err := opt.GetRuntimeFactory()(ctx, &rtCfg)
		if err != nil {
			return fmt.Errorf("failed to create runtime: %w", err)
		}
		runtimeMap[rtCfg.Type] = runtime
	}

	mgr, err := fs.NewManager(*mgrCfg)
	if err != nil {
		return fmt.Errorf("failed to create manager: %w", err)
	}

	handler := &rest.Handler{
		Manager:      mgr,
		EnableTls:    config.EnableTLS,
		TlsCertFile:  config.TLSCertFile,
		TlsKeyFile:   config.TLSKeyFile,
		HttpListener: ln,
		Log:          opt.log,
	}

	go func() {
		if err := handler.Run(); err != nil {
			opt.log.Error("Failed to run REST handler", zap.Error(err))
		}
	}()

	go func() {
		<-ctx.Done()
		if err := handler.Stop(); err != nil {
			opt.log.Error("Failed to stop REST handler", zap.Error(err))
		}
	}()
	return nil
}
