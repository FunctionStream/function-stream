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

package server

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

	"github.com/functionstream/function-stream/common/config"

	"github.com/functionstream/function-stream/fs/runtime/external"

	"github.com/go-logr/logr"

	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/fs/runtime/wazero"
	"github.com/functionstream/function-stream/fs/statestore"
	"github.com/go-openapi/spec"
	"github.com/pkg/errors"
)

var (
	ErrUnsupportedStateStore = errors.New("unsupported state store")
	ErrUnsupportedQueueType  = errors.New("unsupported queue type")
)

type Server struct {
	options       *serverOptions
	httpSvr       atomic.Pointer[http.Server]
	log           *common.Logger
	Manager       fs.FunctionManager
	FunctionStore FunctionStore
}

type TubeLoaderType func(c *FactoryConfig) (contube.TubeFactory, error)
type RuntimeLoaderType func(c *FactoryConfig) (api.FunctionRuntimeFactory, error)
type StateStoreLoaderType func(c *StateStoreConfig) (api.StateStore, error)

type serverOptions struct {
	httpListener           net.Listener
	managerOpts            []fs.ManagerOption
	httpTubeFact           *contube.HttpTubeFactory
	stateStoreLoader       StateStoreLoaderType
	functionStore          string
	enableTls              bool
	tlsCertFile            string
	tlsKeyFile             string
	tubeFactoryBuilders    map[string]func(configMap config.ConfigMap) (contube.TubeFactory, error)
	tubeConfig             map[string]config.ConfigMap
	runtimeFactoryBuilders map[string]func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error)
	runtimeConfig          map[string]config.ConfigMap
	queueConfig            QueueConfig
	log                    *logr.Logger
}

type ServerOption interface {
	apply(option *serverOptions) (*serverOptions, error)
}

type serverOptionFunc func(*serverOptions) (*serverOptions, error)

func (f serverOptionFunc) apply(c *serverOptions) (*serverOptions, error) {
	return f(c)
}

// WithHttpListener sets the listener for the HTTP server.
// If not set, the server will listen on the Config.ListenAddr.
func WithHttpListener(listener net.Listener) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.httpListener = listener
		return o, nil
	})
}

// WithHttpTubeFactory sets the factory for the HTTP tube.
// If not set, the server will use the default HTTP tube factory.
func WithHttpTubeFactory(factory *contube.HttpTubeFactory) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.httpTubeFact = factory
		return o, nil
	})
}

func WithQueueConfig(config QueueConfig) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.queueConfig = config
		return o, nil
	})
}

func WithTubeFactoryBuilder(
	name string,
	builder func(configMap config.ConfigMap) (contube.TubeFactory, error),
) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.tubeFactoryBuilders[name] = builder
		return o, nil
	})
}

func WithTubeFactoryBuilders(
	builder map[string]func(configMap config.ConfigMap,
	) (contube.TubeFactory, error)) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		for n, b := range builder {
			o.tubeFactoryBuilders[n] = b
		}
		return o, nil
	})
}

func WithRuntimeFactoryBuilder(
	name string,
	builder func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error),
) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.runtimeFactoryBuilders[name] = builder
		return o, nil
	})
}

func WithRuntimeFactoryBuilders(
	builder map[string]func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error),
) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		for n, b := range builder {
			o.runtimeFactoryBuilders[n] = b
		}
		return o, nil
	})
}

func WithStateStoreLoader(loader func(c *StateStoreConfig) (api.StateStore, error)) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.stateStoreLoader = loader
		return o, nil
	})
}

func WithPackageLoader(packageLoader api.PackageLoader) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.managerOpts = append(o.managerOpts, fs.WithPackageLoader(packageLoader))
		return o, nil
	})
}

func WithLogger(log *logr.Logger) ServerOption {
	return serverOptionFunc(func(c *serverOptions) (*serverOptions, error) {
		c.log = log
		return c, nil
	})
}

func GetBuiltinTubeFactoryBuilder() map[string]func(configMap config.ConfigMap) (contube.TubeFactory, error) {
	return map[string]func(configMap config.ConfigMap) (contube.TubeFactory, error){
		common.PulsarTubeType: func(configMap config.ConfigMap) (contube.TubeFactory, error) {
			return contube.NewPulsarEventQueueFactory(context.Background(), contube.ConfigMap(configMap))
		},
		//nolint:unparam
		common.MemoryTubeType: func(_ config.ConfigMap) (contube.TubeFactory, error) {
			return contube.NewMemoryQueueFactory(context.Background()), nil
		},
		common.EmptyTubeType: func(_ config.ConfigMap) (contube.TubeFactory, error) {
			return contube.NewEmptyTubeFactory(), nil
		},
	}
}

func GetBuiltinRuntimeFactoryBuilder() map[string]func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error) {
	return map[string]func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error){
		//nolint:unparam
		common.WASMRuntime: func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error) {
			return wazero.NewWazeroFunctionRuntimeFactory(), nil
		},
		common.ExternalRuntime: func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error) {
			return external.NewFactoryWithConfig(configMap)
		},
	}
}

func setupFactories[T any](factoryBuilder map[string]func(configMap config.ConfigMap) (T, error),
	config map[string]config.ConfigMap,
) (map[string]T, error) {
	factories := make(map[string]T)
	for name, builder := range factoryBuilder {
		f, err := builder(config[name])
		if err != nil {
			return nil, fmt.Errorf("error creating factory [%s] %w", name, err)
		}
		factories[name] = f
	}
	return factories, nil
}

func DefaultStateStoreLoader(c *StateStoreConfig) (api.StateStore, error) {
	switch strings.ToLower(*c.Type) {
	case common.StateStorePebble:
		return statestore.NewTmpPebbleStateStore()
	}
	return nil, fmt.Errorf("unsupported state store type [%s] %w", *c.Type, ErrUnsupportedStateStore)
}

func WithConfig(config *Config) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		ln, err := net.Listen("tcp", config.ListenAddr)
		if err != nil {
			return nil, err
		}
		o.httpListener = ln
		o.enableTls = config.EnableTLS
		if o.enableTls {
			if config.TLSCertFile == "" || config.TLSKeyFile == "" {
				return nil, fmt.Errorf("TLS certificate and key file must be provided")
			}
			o.tlsCertFile = config.TLSCertFile
			o.tlsKeyFile = config.TLSKeyFile
		}
		o.tubeConfig = config.TubeConfig
		o.queueConfig = config.Queue
		o.runtimeConfig = config.RuntimeConfig
		if config.StateStore != nil {
			stateStore, err := o.stateStoreLoader(config.StateStore)
			if err != nil {
				return nil, err
			}
			o.managerOpts = append(o.managerOpts, fs.WithStateStore(stateStore))
		}
		o.functionStore = config.FunctionStore
		return o, nil
	})
}

func NewServer(opts ...ServerOption) (*Server, error) {
	options := &serverOptions{}
	options.tubeFactoryBuilders = make(map[string]func(configMap config.ConfigMap) (contube.TubeFactory, error))
	options.tubeConfig = make(map[string]config.ConfigMap)
	options.runtimeFactoryBuilders = make(map[string]func(configMap config.ConfigMap) (api.FunctionRuntimeFactory, error))
	options.runtimeConfig = make(map[string]config.ConfigMap)
	options.stateStoreLoader = DefaultStateStoreLoader
	options.managerOpts = []fs.ManagerOption{}
	for _, o := range opts {
		if o == nil {
			continue
		}
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
	}
	var log *common.Logger
	if options.log == nil {
		log = common.NewDefaultLogger()
	} else {
		log = common.NewLogger(options.log)
	}
	if options.httpTubeFact == nil {
		options.httpTubeFact = contube.NewHttpTubeFactory(context.Background())
		log.Info("Using the default HTTP tube factory")
	}
	options.managerOpts = append(options.managerOpts,
		fs.WithTubeFactory("http", options.httpTubeFact),
		fs.WithLogger(log.Logger))

	// Config Tube Factory
	if tubeFactories, err := setupFactories(options.tubeFactoryBuilders, options.tubeConfig); err == nil {
		for name, f := range tubeFactories {
			options.managerOpts = append(options.managerOpts, fs.WithTubeFactory(name, f))
		}
	} else {
		return nil, err
	}

	// Config Runtime Factory
	if runtimeFactories, err := setupFactories(options.runtimeFactoryBuilders, options.runtimeConfig); err == nil {
		for name, f := range runtimeFactories {
			options.managerOpts = append(options.managerOpts, fs.WithRuntimeFactory(name, f))
		}
	} else {
		return nil, err
	}

	// Config Queue Factory
	if options.queueConfig.Type != "" {
		queueFactoryBuilder, ok := options.tubeFactoryBuilders[options.queueConfig.Type]
		if !ok {
			return nil, fmt.Errorf("%w, queueType: %s", ErrUnsupportedQueueType, options.queueConfig.Type)
		}
		queueFactory, err := queueFactoryBuilder(options.queueConfig.Config)
		if err != nil {
			return nil, fmt.Errorf("error creating queue factory %w", err)
		}
		options.managerOpts = append(options.managerOpts, fs.WithQueueFactory(queueFactory))
	}

	manager, err := fs.NewFunctionManager(options.managerOpts...)
	if err != nil {
		return nil, err
	}
	if options.httpListener == nil {
		options.httpListener, err = net.Listen("tcp", "localhost:7300")
		if err != nil {
			return nil, err
		}
	}
	var functionStore FunctionStore
	if options.functionStore != "" {
		functionStore, err = NewFunctionStoreImpl(manager, options.functionStore)
		if err != nil {
			return nil, err
		}
	} else {
		functionStore = NewFunctionStoreDisabled()
	}
	err = functionStore.Load()
	if err != nil {
		return nil, err
	}
	return &Server{
		options:       options,
		Manager:       manager,
		log:           log,
		FunctionStore: functionStore,
	}, nil
}

func NewDefaultServer() (*Server, error) {
	defaultConfig := &Config{
		ListenAddr: ":7300",
		Queue: QueueConfig{
			Type:   common.MemoryTubeType,
			Config: config.ConfigMap{},
		},
		TubeConfig: map[string]config.ConfigMap{
			common.PulsarTubeType: {
				contube.PulsarURLKey: "pulsar://localhost:6650",
			},
		},
		RuntimeConfig: map[string]config.ConfigMap{},
	}
	return NewServer(
		WithTubeFactoryBuilders(GetBuiltinTubeFactoryBuilder()),
		WithRuntimeFactoryBuilders(GetBuiltinRuntimeFactoryBuilder()),
		WithConfig(defaultConfig))
}

func (s *Server) Run(context context.Context) {
	s.log.Info("Hello from the function stream server!")
	go func() {
		<-context.Done()
		err := s.Close()
		if err != nil {
			s.log.Error(err, "failed to shutdown server")
			return
		}
	}()
	err := s.startRESTHandlers()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error(err, "Error starting REST handlers")
	}
}

func (s *Server) startRESTHandlers() error {

	statusSvr := new(restful.WebService)
	statusSvr.Path("/api/v1/status")
	statusSvr.Route(statusSvr.GET("/").To(func(request *restful.Request, response *restful.Response) {
		response.WriteHeader(http.StatusOK)
	}).
		Doc("Get the status of the Function Stream").
		Metadata(restfulspec.KeyOpenAPITags, []string{"status"}).
		Operation("getStatus"))

	container := restful.NewContainer()
	container.Add(s.makeFunctionService())
	container.Add(s.makeTubeService())
	container.Add(s.makeStateService())
	container.Add(s.makeHttpTubeService())
	container.Add(s.makeFunctionStoreService())
	container.Add(statusSvr)

	cors := restful.CrossOriginResourceSharing{
		AllowedHeaders: []string{"Content-Type", "Accept"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS", "PUT", "DELETE"},
		CookiesAllowed: false,
		Container:      container}
	container.Filter(cors.Filter)
	container.Filter(container.OPTIONSFilter)

	config := restfulspec.Config{
		WebServices:                   container.RegisteredWebServices(),
		APIPath:                       "/apidocs",
		PostBuildSwaggerObjectHandler: enrichSwaggerObject}
	container.Add(restfulspec.NewOpenAPIService(config))

	httpSvr := &http.Server{
		Handler: container.ServeMux,
	}
	s.httpSvr.Store(httpSvr)

	if s.options.enableTls {
		return httpSvr.ServeTLS(s.options.httpListener, s.options.tlsCertFile, s.options.tlsKeyFile)
	} else {
		return httpSvr.Serve(s.options.httpListener)
	}
}

func enrichSwaggerObject(swo *spec.Swagger) {
	swo.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:       "Function Stream Service",
			Description: "Manage Function Stream Resources",
			Contact: &spec.ContactInfo{
				ContactInfoProps: spec.ContactInfoProps{
					Name: "Function Stream Org",
					URL:  "https://github.com/FunctionStream",
				},
			},
			License: &spec.License{
				LicenseProps: spec.LicenseProps{
					Name: "Apache 2",
					URL:  "http://www.apache.org/licenses/",
				},
			},
			Version: "1.0.0",
		},
	}
	swo.Host = "localhost:7300"
	swo.Schemes = []string{"http"}
	swo.Tags = []spec.Tag{
		{
			TagProps: spec.TagProps{
				Name:        "function",
				Description: "Managing functions"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "tube",
				Description: "Managing tubes"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "state",
				Description: "Managing state"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "http-tube",
				Description: "Managing HTTP tubes"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "function-store",
				Description: "Managing function store"},
		},
	}
}

func (s *Server) WaitForReady(ctx context.Context) <-chan struct{} {
	c := make(chan struct{})
	detect := func() bool {
		u := (&url.URL{
			Scheme: "http",
			Host:   s.options.httpListener.Addr().String(),
			Path:   "/api/v1/status",
		}).String()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			s.log.Error(err, "Failed to create detect request")
			return false
		}
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			s.log.Info("Detect connection to server failed", "error", err)
		}
		s.log.Info("Server is ready", "address", s.options.httpListener.Addr().String())
		return true
	}
	go func() {
		defer close(c)

		if detect() {
			return
		}
		// Try to connect to the server
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				if detect() {
					return
				}
			}
		}
	}()
	return c
}

func (s *Server) Close() error {
	s.log.Info("Shutting down function stream server")
	if httpSvr := s.httpSvr.Load(); httpSvr != nil {
		if err := httpSvr.Close(); err != nil {
			return err
		}
	}
	if s.Manager != nil {
		err := s.Manager.Close()
		if err != nil {
			return err
		}
	}
	s.log.Info("Function stream server is shut down")
	return nil
}

func (s *Server) handleRestError(e error) {
	if e == nil {
		return
	}
	s.log.Error(e, "Error handling REST request")
}
