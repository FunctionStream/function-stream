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
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/go-openapi/spec"
	"github.com/pkg/errors"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type Server struct {
	options *serverOptions
	httpSvr atomic.Pointer[http.Server]
	log     *slog.Logger
	Manager *fs.FunctionManager
}

type serverOptions struct {
	httpListener net.Listener
	managerOpts  []fs.ManagerOption
	httpTubeFact *contube.HttpTubeFactory
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

func NewServer(opts ...ServerOption) (*Server, error) {
	options := &serverOptions{}
	httpTubeFact := contube.NewHttpTubeFactory(context.Background())
	options.managerOpts = []fs.ManagerOption{
		fs.WithDefaultTubeFactory(contube.NewMemoryQueueFactory(context.Background())),
		fs.WithTubeFactory("http", httpTubeFact),
	}
	options.httpTubeFact = httpTubeFact
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
	return &Server{
		options: options,
		Manager: manager,
		log:     slog.With(),
	}, nil
}

func NewServerWithConfig(config *common.Config) (*Server, error) {
	ln, err := net.Listen("tcp", config.ListenAddr)
	if err != nil {
		return nil, err
	}

	var tubeFactory contube.TubeFactory
	switch config.TubeType {
	case common.PulsarTubeType:
		tubeFactory, err = contube.NewPulsarEventQueueFactory(context.Background(), (&contube.PulsarTubeFactoryConfig{
			PulsarURL: config.PulsarURL,
		}).ToConfigMap())
		if err != nil {
			return nil, errors.Wrap(err, "failed to create default pulsar tube factory")
		}
	case common.MemoryTubeType:
		tubeFactory = contube.NewMemoryQueueFactory(context.Background())
	}
	managerOpts := []fs.ManagerOption{
		fs.WithDefaultTubeFactory(tubeFactory),
	}

	return NewServer(
		WithHttpListener(ln),
		WithFunctionManager(managerOpts...),
	)
}

func NewDefaultServer() (*Server, error) {
	return NewServerWithConfig(LoadConfigFromEnv())
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
