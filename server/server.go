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
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/restclient"
	"github.com/go-openapi/spec"
	"github.com/gorilla/mux"
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

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS, PUT, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Content-Type, Content-Length, Accept-Encoding, X-CSRF-Token, Authorization")

		if r.Method == "OPTIONS" {
			w.Header().Set("Access-Control-Max-Age", "86400")
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) startRESTHandlers() error {
	r := mux.NewRouter()

	r.PathPrefix("/").Handler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
	})).Methods("OPTIONS")

	r.Use(corsMiddleware)

	r.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods("GET")

	//r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	functionName := vars["function_name"]
	//	log := s.log.With(slog.String("name", functionName), slog.String("phase", "starting"))
	//	log.Debug("Starting function")
	//
	//	body, err := io.ReadAll(r.Body)
	//	if err != nil {
	//		log.Error("Failed to read body", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//
	//	if len(body) == 0 {
	//		log.Debug("The body is empty")
	//		http.Error(w, "The body is empty. You should provide the function definition", http.StatusBadRequest)
	//		return
	//	}
	//
	//	var function restclient.Function
	//	err = json.Unmarshal(body, &function)
	//	if err != nil {
	//		log.Error("Failed to parse function definition", "error", err)
	//		http.Error(w, fmt.Errorf("failed to parse function definition: %w", err).Error(), http.StatusBadRequest)
	//		return
	//	}
	//	function.Name = &functionName
	//
	//	f, err := constructFunction(&function)
	//	if err != nil {
	//		log.Error("Failed to construct function", "error", err)
	//		http.Error(w, err.Error(), http.StatusBadRequest)
	//		return
	//	}
	//
	//	err = s.Manager.StartFunction(f)
	//	if err != nil {
	//		log.Error("Failed to start function", "error", err)
	//		http.Error(w, err.Error(), http.StatusBadRequest)
	//		return
	//	}
	//	log.Info("Started function")
	//}).Methods("POST")
	//
	//r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	functionName := vars["function_name"]
	//	log := s.log.With(slog.String("name", functionName), slog.String("phase", "deleting"))
	//
	//	err := s.Manager.DeleteFunction(functionName)
	//	if errors.Is(err, common.ErrorFunctionNotFound) {
	//		log.Error("Function not found", "error", err)
	//		http.Error(w, err.Error(), http.StatusNotFound)
	//		return
	//	}
	//	log.Info("Deleted function")
	//}).Methods("DELETE")
	//
	//r.HandleFunc("/api/v1/functions", func(w http.ResponseWriter, r *http.Request) {
	//	log := s.log.With()
	//	log.Info("Listing functions")
	//	functions := s.Manager.ListFunctions()
	//	w.Header().Set("Content-Type", "application/json")
	//	err := json.NewEncoder(w).Encode(functions)
	//	if err != nil {
	//		slog.Error("Error when listing functions", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//}).Methods("GET")

	//r.HandleFunc("/api/v1/produce/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	queueName := vars["queue_name"]
	//	log := s.log.With(slog.String("queue_name", queueName))
	//	log.Info("Producing event to queue")
	//	content, err := io.ReadAll(r.Body)
	//	if err != nil {
	//		log.Error("Failed to read body", "error", err)
	//		http.Error(w, errors.Wrap(err, "Failed to read body").Error(), http.StatusBadRequest)
	//		return
	//	}
	//	err = s.Manager.ProduceEvent(queueName, contube.NewRecordImpl(content, func() {}))
	//	if err != nil {
	//		log.Error("Failed to produce event", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//	log.Info("Produced event to queue")
	//}).Methods("PUT")
	//
	//r.HandleFunc("/api/v1/consume/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	queueName := vars["queue_name"]
	//	log := s.log.With(slog.String("queue_name", queueName))
	//	log.Info("Consuming event from queue")
	//	event, err := s.Manager.ConsumeEvent(queueName)
	//	if err != nil {
	//		log.Error("Failed to consume event", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//	w.Header().Set("Content-Type", "application/json")
	//	err = json.NewEncoder(w).Encode(string(event.GetPayload()))
	//	if err != nil {
	//		log.Error("Error when encoding event", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//	log.Info("Consumed event from queue")
	//}).Methods("GET")
	//
	//if s.options.httpTubeFact != nil {
	//	r.HandleFunc("/api/v1/http-tube/{endpoint}", s.options.httpTubeFact.GetHandleFunc(func(r *http.Request) (string, error) {
	//		e, ok := mux.Vars(r)["endpoint"]
	//		if !ok {
	//			return "", errors.New("endpoint not found")
	//		}
	//		return e, nil
	//	}, s.log)).Methods("POST")
	//}
	//
	//r.HandleFunc("/api/v1/state/{key}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	key, ok := vars["key"]
	//	if !ok {
	//		http.Error(w, "Key not found", http.StatusBadRequest)
	//		return
	//	}
	//	log := s.log.With(slog.String("key", key))
	//	log.Info("Getting state")
	//	state := s.Manager.GetStateStore()
	//	if state == nil {
	//		log.Error("No state store configured")
	//		http.Error(w, "No state store configured", http.StatusBadRequest)
	//		return
	//	}
	//	content, err := io.ReadAll(r.Body)
	//	if err != nil {
	//		log.Error("Failed to read body", "error", err)
	//		http.Error(w, errors.Wrap(err, "Failed to read body").Error(), http.StatusBadRequest)
	//		return
	//	}
	//	err = state.PutState(key, content)
	//	if err != nil {
	//		log.Error("Failed to put state", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//}).Methods("POST")
	//
	//r.HandleFunc("/api/v1/state/{key}", func(w http.ResponseWriter, r *http.Request) {
	//	vars := mux.Vars(r)
	//	key, ok := vars["key"]
	//	if !ok {
	//		http.Error(w, "Key not found", http.StatusBadRequest)
	//		return
	//	}
	//	log := s.log.With(slog.String("key", key))
	//	log.Info("Getting state")
	//	state := s.Manager.GetStateStore()
	//	if state == nil {
	//		log.Error("No state store configured")
	//		http.Error(w, "No state store configured", http.StatusBadRequest)
	//		return
	//	}
	//	content, err := state.GetState(key)
	//	if err != nil {
	//		log.Error("Failed to get state", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//	w.Header().Set("Content-Type", "application/octet-stream") // Set the content type to application/octet-stream for binary data
	//	_, err = w.Write(content)
	//	if err != nil {
	//		log.Error("Failed to write to response", "error", err)
	//		http.Error(w, err.Error(), http.StatusInternalServerError)
	//		return
	//	}
	//}).Methods("GET")

	container := restful.NewContainer()
	container.Add(s.makeFunctionService())
	container.Add(s.makeTubeService())
	container.Add(s.makeStateService())

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

func constructFunction(function *restclient.Function) (*model.Function, error) {
	if function.Name == nil {
		return nil, errors.New("function name is required")
	}
	f := &model.Function{
		Name:     *function.Name,
		Inputs:   function.Inputs,
		Output:   function.Output,
		Replicas: function.Replicas,
	}
	if function.Runtime != nil {
		f.Runtime = &model.RuntimeConfig{
			Type:   function.Runtime.Type.Get(),
			Config: function.Runtime.Config,
		}
	}
	if function.Config != nil {
		f.Config = *function.Config
	} else {
		f.Config = make(map[string]string)
	}
	return f, nil
}
