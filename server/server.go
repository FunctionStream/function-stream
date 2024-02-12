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
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib"
	"github.com/functionstream/functionstream/lib/contube"
	"github.com/functionstream/functionstream/restclient"
	"github.com/gorilla/mux"
	"github.com/pkg/errors"
	"io"
	"log/slog"
	"net/http"
	"sync/atomic"
)

type Server struct {
	manager *lib.FunctionManager
	config  *lib.Config
	httpSvr atomic.Pointer[http.Server]
}

func New(config *lib.Config) *Server {
	manager, err := lib.NewFunctionManager(config)
	if err != nil {
		slog.Error("Error creating function manager", err)
	}
	return &Server{
		manager: manager,
		config:  config,
	}
}

func (s *Server) Run(context context.Context) {
	slog.Info("Hello, Function Stream!")
	go func() {
		<-context.Done()
		err := s.Close()
		if err != nil {
			slog.Error("Error shutting down server", "error", err)
			return
		}
	}()
	err := s.startRESTHandlers()
	if err != nil {
		slog.Error("Error starting REST handlers", "error", err)
	}
}

func (s *Server) startRESTHandlers() error {
	r := mux.NewRouter()
	r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["function_name"]

		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		if len(body) == 0 {
			http.Error(w, "The body is empty. You should provide the function definition", http.StatusBadRequest)
			return
		}

		var function restclient.Function
		err = json.Unmarshal(body, &function)
		if err != nil {
			http.Error(w, fmt.Errorf("failed to parse function definition: %w", err).Error(), http.StatusBadRequest)
			return
		}
		function.Name = &functionName

		f, err := constructFunction(&function)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}

		slog.Info("Starting function", slog.Any("name", functionName))
		err = s.manager.StartFunction(f)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		slog.Info("Started function", slog.Any("name", functionName))
	}).Methods("POST")

	r.HandleFunc("/api/v1/function/{function_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["function_name"]
		slog.Info("Deleting function", slog.Any("name", functionName))

		err := s.manager.DeleteFunction(functionName)
		if errors.Is(err, common.ErrorFunctionNotFound) {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		slog.Info("Deleted function", slog.Any("name", functionName))
	}).Methods("DELETE")

	r.HandleFunc("/api/v1/functions", func(w http.ResponseWriter, r *http.Request) {
		functions := s.manager.ListFunctions()
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
		slog.Info("Producing event to queue", slog.Any("queue_name", queueName))
		content, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, errors.Wrap(err, "Failed  to read body").Error(), http.StatusBadRequest)
			return
		}
		err = s.manager.ProduceEvent(queueName, contube.NewRecordImpl(content, func() {}))
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}).Methods("PUT")

	r.HandleFunc("/api/v1/consume/{queue_name}", func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		queueName := vars["queue_name"]
		slog.Info("Consuming event from queue", slog.Any("queue_name", queueName))
		event, err := s.manager.ConsumeEvent(queueName)
		if err != nil {
			slog.Error("Error when consuming event", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		err = json.NewEncoder(w).Encode(string(event.GetPayload()))
		if err != nil {
			slog.Error("Error when encoding event", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
	}).Methods("GET")

	httpSvr := &http.Server{
		Addr:    s.config.ListenAddr,
		Handler: r,
	}
	s.httpSvr.Store(httpSvr)

	return httpSvr.ListenAndServe()
}

func (s *Server) Close() error {
	slog.Info("Shutting down function stream server")
	if httpSvr := s.httpSvr.Load(); httpSvr != nil {
		if err := httpSvr.Close(); err != nil {
			return err
		}
	}
	return nil
}

func constructFunction(function *restclient.Function) (*model.Function, error) {
	if function.Name == nil {
		return nil, errors.New("function name is required")
	}
	f := &model.Function{
		Name:    *function.Name,
		Archive: function.Archive,
		Inputs:  function.Inputs,
		Output:  function.Output,
	}
	if function.Replicas != nil {
		f.Replicas = *function.Replicas
	} else {
		f.Replicas = 1
	}
	if function.Config != nil {
		f.Config = *function.Config
	} else {
		f.Config = make(map[string]string)
	}
	return f, nil
}
