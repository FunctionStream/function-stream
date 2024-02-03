package server

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/functionstream/functionstream/common"
	"github.com/functionstream/functionstream/common/model"
	"github.com/functionstream/functionstream/lib"
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

		var function model.Function
		err = json.Unmarshal(body, &function)
		if err != nil {
			http.Error(w, fmt.Errorf("failed to parse function definition: %w", err).Error(), http.StatusBadRequest)
			return
		}
		function.Name = functionName

		slog.Info("Starting function", slog.Any("name", functionName))
		err = s.manager.StartFunction(function)
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
