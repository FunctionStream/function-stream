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
	"encoding/json"
	"fmt"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/functionstream/function-stream/fs/contube"
	"github.com/functionstream/function-stream/restclient"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type Server struct {
	options *serverOptions
	config  *common.Config
	httpSvr atomic.Pointer[http.Server]
	log     *slog.Logger
}

type serverOptions struct {
	httpListener net.Listener
	manager      *fs.FunctionManager
	httpTube     *contube.HttpTubeFactory
}

type ServerOption interface {
	apply(option *serverOptions) (*serverOptions, error)
}

type serverOptionFunc func(*serverOptions) (*serverOptions, error)

func (f serverOptionFunc) apply(c *serverOptions) (*serverOptions, error) {
	return f(c)
}

func WithFunctionManager(manager *fs.FunctionManager) ServerOption {
	return serverOptionFunc(func(o *serverOptions) (*serverOptions, error) {
		o.manager = manager
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

func (s *serverOptions) newDefaultFunctionManager(config *common.Config) (*fs.FunctionManager, error) {
	var tubeFactory contube.TubeFactory
	var err error
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
	s.httpTube = contube.NewHttpTubeFactory(context.Background()).(*contube.HttpTubeFactory)
	manager, err := fs.NewFunctionManager(
		fs.WithDefaultTubeFactory(tubeFactory),
		fs.WithTubeFactory("http", s.httpTube),
	)
	if err != nil {
		return nil, errors.Wrap(err, "failed to create default function manager")
	}
	return manager, nil
}

func NewServer(config *common.Config, opts ...ServerOption) (*Server, error) {
	options := &serverOptions{}
	for _, o := range opts {
		_, err := o.apply(options)
		if err != nil {
			return nil, err
		}
	}
	if options.manager == nil {
		manager, err := options.newDefaultFunctionManager(config)
		if err != nil {
			return nil, err
		}
		options.manager = manager
	}

	return &Server{
		options: options,
		config:  config,
		log:     slog.With(),
	}, nil
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
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/status", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}).Methods("GET")

	r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["function_name"]
		log := s.log.With(slog.String("name", functionName), slog.String("phase", "starting"))
		log.Debug("Starting function")

		body, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error("Failed to read body", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(body) == 0 {
			log.Debug("The body is empty")
			http.Error(w, "The body is empty. You should provide the function definition", http.StatusBadRequest)
			return
		}

		var function restclient.Function
		err = json.Unmarshal(body, &function)
		if err != nil {
			log.Error("Failed to parse function definition", "error", err)
			http.Error(w, fmt.Errorf("failed to parse function definition: %w", err).Error(), http.StatusBadRequest)
			return
		}
		function.Name = &functionName

		f, err := ConstructFunction(&function)
		if err != nil {
			log.Error("Failed to construct function", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		err = s.options.manager.StartFunction(f)
		if err != nil {
			log.Error("Failed to start function", "error", err)
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		log.Info("Started function")
	}).Methods("POST")

	r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["function_name"]
		log := s.log.With(slog.String("name", functionName), slog.String("phase", "deleting"))

		err := s.options.manager.DeleteFunction(functionName)
		if errors.Is(err, common.ErrorFunctionNotFound) {
			log.Error("Function not found", "error", err)
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		log.Info("Deleted function")
	}).Methods("DELETE")

	r.HandleFunc("/api/v1/functions", func(w http.ResponseWriter, r *http.Request) {
		log := s.log.With()
		log.Info("Listing functions")
		functions := s.options.manager.ListFunctions()
		w.Header().Set("Content-Type", "application/json")
		err := json.NewEncoder(w).Encode(functions)
		if err != nil {
			slog.Error("Error when listing functions", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}).Methods("GET")

	r.HandleFunc("/api/v1/produce/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		queueName := vars["queue_name"]
		log := s.log.With(slog.String("queue_name", queueName))
		log.Info("Producing event to queue")
		content, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error("Failed to read body", "error", err)
			http.Error(w, errors.Wrap(err, "Failed to read body").Error(), http.StatusBadRequest)
			return
		}
		err = s.options.manager.ProduceEvent(queueName, contube.NewRecordImpl(content, func() {}))
		if err != nil {
			log.Error("Failed to produce event", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Info("Produced event to queue")
	}).Methods("PUT")

	r.HandleFunc("/api/v1/consume/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		queueName := vars["queue_name"]
		log := s.log.With(slog.String("queue_name", queueName))
		log.Info("Consuming event from queue")
		event, err := s.options.manager.ConsumeEvent(queueName)
		if err != nil {
			log.Error("Failed to consume event", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(string(event.GetPayload()))
		if err != nil {
			log.Error("Error when encoding event", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Info("Consumed event from queue")
	}).Methods("GET")

	r.HandleFunc("/api/v1/http-tube/{endpoint}", s.options.httpTube.GetHandleFunc(func(r *http.Request) (string, error) {
		e, ok := mux.Vars(r)["endpoint"]
		if !ok {
			return "", errors.New("endpoint not found")
		}
		return e, nil
	}, s.log)).Methods("POST")

	httpSvr := &http.Server{
		Addr:    s.config.ListenAddr,
		Handler: r,
	}
	s.httpSvr.Store(httpSvr)

	if s.options.httpListener != nil {
		return httpSvr.Serve(s.options.httpListener)
	} else {
		return httpSvr.ListenAndServe()
	}
}

func (s *Server) WaitForReady(ctx context.Context) <-chan struct{} {
	c := make(chan struct{})
	detect := func() bool {
		u := (&url.URL{
			Scheme: "http",
			Host:   s.config.ListenAddr,
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
					s.log.Info("Server is ready", slog.String("address", s.config.ListenAddr))
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
	s.log.Info("Function stream server is shut down")
	return nil
}

func ConstructFunction(function *restclient.Function) (*model.Function, error) {
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
