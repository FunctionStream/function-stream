/*
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
	"encoding/json"
	"errors"
	"fmt"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib"
	"github.com/functionstream/functionstream/restclient"
	"github.com/gorilla/mux"
	"io"
	"log/slog"
	"net/http"
)

type Server struct {
	manager *lib.FunctionManager
}

func New() *Server {
	manager, err := lib.NewFunctionManager()
	if err != nil {
		slog.Error("Error creating function manager", err)
	}
	return &Server{
		manager: manager,
	}
}

func (s *Server) Run() {
	slog.Info("Hello, Function Stream!")
	err := s.startRESTHandlers()
	if err != nil {
		slog.Error("Error starting REST handlers", err)
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
	}).Methods("DELETE")

	return http.ListenAndServe(common.GetConfig().ListenAddr, r)
}

func (s *Server) Close() error {
	slog.Info("Shutting down function stream server")
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