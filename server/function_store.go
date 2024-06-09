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
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"

	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/common/model"
	"github.com/functionstream/function-stream/fs"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type FunctionStore interface {
	Load() error
}

type FunctionStoreImpl struct {
	mu               sync.Mutex
	fm               fs.FunctionManager
	path             string
	loadedFunctions  map[string]*model.Function
	loadingFunctions map[string]*model.Function
}

func (f *FunctionStoreImpl) Load() error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.loadingFunctions = make(map[string]*model.Function)
	info, err := os.Stat(f.path)
	if err != nil {
		if os.IsNotExist(err) {
			slog.Info("the path to the function store does not exist. skip loading functions")
			return nil
		}
		return errors.Wrapf(err, "the path to the function store %s is invalid", f.path)
	}
	if !info.IsDir() {
		err = f.loadFile(f.path)
		if err != nil {
			return err
		}
	} else {
		err = filepath.Walk(f.path, func(path string, info os.FileInfo, err error) error {
			if err != nil {
				return err
			}

			if strings.HasSuffix(info.Name(), ".yaml") || strings.HasSuffix(info.Name(), ".yml") {
				err := f.loadFile(path)
				if err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	for key, value := range f.loadingFunctions {
		if _, exists := f.loadedFunctions[key]; !exists {
			err := f.fm.StartFunction(value)
			if err != nil {
				return err
			}
		}
	}

	for key, value := range f.loadedFunctions {
		if _, exists := f.loadingFunctions[key]; !exists {
			err := f.fm.DeleteFunction(value.Namespace, value.Name)
			if err != nil {
				return err
			}
		}
	}

	f.loadedFunctions = f.loadingFunctions
	slog.Info("functions loaded", "loadedFunctionsCount", len(f.loadedFunctions))
	return nil
}

func (f *FunctionStoreImpl) loadFile(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		return err
	}
	dec := yaml.NewDecoder(strings.NewReader(string(data)))
	for {
		var function model.Function
		err := dec.Decode(&function)
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
		if err := function.Validate(); err != nil {
			return errors.Wrapf(err, "function %s is invalid", function.Name)
		}
		if _, ok := f.loadingFunctions[function.Name]; ok {
			return errors.Errorf("duplicated function %s", function.Name)
		}
		f.loadingFunctions[function.Name] = &function
	}
	return nil
}

func NewFunctionStoreImpl(fm fs.FunctionManager, path string) (FunctionStore, error) {
	return &FunctionStoreImpl{
		fm:   fm,
		path: path,
	}, nil
}

type FunctionStoreDisabled struct {
}

func (f *FunctionStoreDisabled) Load() error {
	return nil
}

func NewFunctionStoreDisabled() FunctionStore {
	return &FunctionStoreDisabled{}
}

func (s *Server) makeFunctionStoreService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/function-store")

	tags := []string{"function-store"}

	ws.Route(ws.GET("/reload").
		To(func(request *restful.Request, response *restful.Response) {
			err := s.FunctionStore.Load()
			if err != nil {
				s.handleRestError(response.WriteErrorString(400, err.Error()))
				return
			}
			response.WriteHeader(200)
		}).
		Doc("reload functions from the function store").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("reloadFunctions").
		Returns(http.StatusOK, "OK", nil))

	return ws
}
