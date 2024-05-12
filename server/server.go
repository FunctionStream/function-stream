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
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync/atomic"
	"time"

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
	"k8s.io/utils/set"
)

var (
	ErrUnsupportedTRuntimeType = errors.New("unsupported runtime type")
	ErrUnsupportedTubeType     = errors.New("unsupported tube type")
	ErrUnsupportedStateStore   = errors.New("unsupported state store")
)

type Server struct {
	options       *serverOptions
	httpSvr       atomic.Pointer[http.Server]
	log           *slog.Logger
	Manager       fs.FunctionManager
	FunctionStore FunctionStore
}

type TubeLoaderType func(c *FactoryConfig) (contube.TubeFactory, error)
type RuntimeLoaderType func(c *FactoryConfig) (api.FunctionRuntimeFactory, error)
type StateStoreLoaderType func(c *StateStoreConfig) (api.StateStore, error)

type serverOptions struct {
	httpListener     net.Listener
	managerOpts      []fs.ManagerOption
	httpTubeFact     *contube.HttpTubeFactory
	tubeLoader       TubeLoaderType
	runtimeLoader    RuntimeLoaderType
	stateStoreLoader StateStoreLoaderType
	functionStore    string
}

type ServerOption interface {
	apply(option *serverOptions) (*serverOptions, error)
}

type serverOptionFunc func(*serverOptions) (*serverOptions, error)

func (f serverOptionFunc) apply(c *serverOptions) (*serverOptions, error) {
	return f(c)
}

// WithFunctionManager sets the function Manager for the server.
func WithFunctionManager(opts ...fs.ManagerOption) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.managerOpts = append(o.managerOpts, opts...)
		return o, nil
	})
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

// WithTubeLoader sets the loader for the tube factory.
// This must be called before WithConfig.
func WithTubeLoader(loader TubeLoaderType) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.tubeLoader = loader
		return o, nil
	})
}

// WithRuntimeLoader sets the loader for the runtime factory.
// This must be called before WithConfig.
func WithRuntimeLoader(loader RuntimeLoaderType) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.runtimeLoader = loader
		return o, nil
	})
}

func WithStateStoreLoader(loader func(c *StateStoreConfig) (api.StateStore, error)) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.stateStoreLoader = loader
		return o, nil
	})
}

func getRefFactory(m map[string]*FactoryConfig, name string, visited set.Set[string]) (string, error) {
	if visited.Has(name) {
		return "", errors.Errorf("circular reference of factory %s", name)
	}
	visited.Insert(name)
	f, ok := m[name]
	if !ok {
		return "", errors.Errorf("tube factory %s not found", name)
	}
	if f.Ref != nil {
		return getRefFactory(m, strings.ToLower(*f.Ref), visited)
	}
	return name, nil
}

func initFactories[T any](m map[string]*FactoryConfig, newFactory func(c *FactoryConfig) (T, error),
	setup func(n string, f T)) error {
	factoryMap := make(map[string]T)

	for name := range m {
		refName, err := getRefFactory(m, name, set.New[string]())
		if err != nil {
			return err
		}
		if _, ok := factoryMap[refName]; !ok {
			fc, exist := m[refName]
			if !exist {
				return errors.Errorf("factory %s not found, which the factory %s is pointed to", refName, name)
			}
			if fc.Type == nil {
				return errors.Errorf("factory %s type is not set", refName)
			}
			f, err := newFactory(fc)
			if err != nil {
				return err
			}
			factoryMap[refName] = f
		}
		factoryMap[name] = factoryMap[refName]
		setup(name, factoryMap[name])
	}
	return nil
}

func DefaultTubeLoader(c *FactoryConfig) (contube.TubeFactory, error) {
	switch strings.ToLower(*c.Type) {
	case common.PulsarTubeType:
		return contube.NewPulsarEventQueueFactory(context.Background(), contube.ConfigMap(*c.Config))
	case common.MemoryTubeType:
		return contube.NewMemoryQueueFactory(context.Background()), nil
	}
	return nil, errors.WithMessagef(ErrUnsupportedTubeType, "unsupported tube type :%s", *c.Type)
}

func DefaultRuntimeLoader(c *FactoryConfig) (api.FunctionRuntimeFactory, error) {
	switch strings.ToLower(*c.Type) {
	case common.WASMRuntime:
		return wazero.NewWazeroFunctionRuntimeFactory(), nil
	}
	return nil, errors.WithMessagef(ErrUnsupportedTRuntimeType, "unsupported runtime type: %s", *c.Type)
}

func DefaultStateStoreLoader(c *StateStoreConfig) (api.StateStore, error) {
	switch strings.ToLower(*c.Type) {
	case common.StateStorePebble:
		return statestore.NewTmpPebbleStateStore()
	}
	return nil, errors.WithMessagef(ErrUnsupportedStateStore, "unsupported state store type: %s", *c.Type)
}

func WithConfig(config *Config) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		ln, err := net.Listen("tcp", config.ListenAddr)
		if err != nil {
			return nil, err
		}
		o.httpListener = ln
		err = initFactories[contube.TubeFactory](config.TubeFactory, o.tubeLoader, func(n string, f contube.TubeFactory) {
			o.managerOpts = append(o.managerOpts, fs.WithTubeFactory(n, f))
		})
		if err != nil {
			return nil, err
		}
		err = initFactories[api.FunctionRuntimeFactory](config.RuntimeFactory, o.runtimeLoader,
			func(n string, f api.FunctionRuntimeFactory) {
				o.managerOpts = append(o.managerOpts, fs.WithRuntimeFactory(n, f))
			})
		if err != nil {
			return nil, err
		}
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
	httpTubeFact := contube.NewHttpTubeFactory(context.Background())
	options.managerOpts = []fs.ManagerOption{
		fs.WithDefaultTubeFactory(contube.NewMemoryQueueFactory(context.Background())),
		fs.WithTubeFactory("http", httpTubeFact),
	}
	options.httpTubeFact = httpTubeFact
	options.tubeLoader = DefaultTubeLoader
	options.runtimeLoader = DefaultRuntimeLoader
	options.stateStoreLoader = DefaultStateStoreLoader
	for _, o := range opts {
		if o == nil {
			continue
		}
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
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
		log:           slog.With(),
		FunctionStore: functionStore,
	}, nil
}

func NewDefaultServer() (*Server, error) {
	defaultConfig := &Config{
		ListenAddr: ":7300",
		TubeFactory: map[string]*FactoryConfig{
			"pulsar": {
				Type: common.OptionalStr(common.PulsarTubeType),
				Config: &common.ConfigMap{
					contube.PulsarURLKey: "pulsar://localhost:6650",
				},
			},
			"default": {
				Ref: common.OptionalStr("pulsar"),
			},
		},
		RuntimeFactory: map[string]*FactoryConfig{
			"wasm": {
				Type: common.OptionalStr(common.WASMRuntime),
			},
			"default": {
				Ref: common.OptionalStr("wasm"),
			},
		},
	}
	return NewServer(WithConfig(defaultConfig))
}

func (s *Server) Run(context context.Context) {
	s.log.Info("Hello, Function Stream!")
	go func() {
		<-context.Done()
		err := s.Close()
		if err != nil {
			s.log.Error("Error shutting down server", "error", err)
			return
		}
	}()
	err := s.startRESTHandlers()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		s.log.Error("Error starting REST handlers", "error", err)
	}
}

func (s *Server) startRESTHandlers() error {

	statusSvr := new(restful.WebService)
	statusSvr.Route(statusSvr.GET("/api/v1/status").To(func(request *restful.Request, response *restful.Response) {
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

	return httpSvr.Serve(s.options.httpListener)
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
			s.log.InfoContext(ctx, "Failed to create detect request", slog.Any("error", err))
			return false
		}
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			s.log.InfoContext(ctx, "Detect connection to server failed", slog.Any("error", err))
		}
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
					s.log.Info("Server is ready", slog.String("address", s.options.httpListener.Addr().String()))
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
	s.log.Error("Error handling REST request", "error", e)
}
