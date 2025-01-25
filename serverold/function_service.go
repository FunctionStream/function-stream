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

package serverold

import (
	"errors"
	"net/http"

	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/common"
	"github.com/functionstream/function-stream/common/model"
)

func (s *Server) makeFunctionService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/function").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"function"}

	ws.Route(ws.GET("/").
		To(func(request *restful.Request, response *restful.Response) {
			functions := s.Manager.ListFunctions()
			s.handleRestError(response.WriteEntity(functions))
		}).
		Doc("get all functions").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getAllFunctions").
		Returns(http.StatusOK, "OK", []string{}).
		Writes([]string{}))

	ws.Route(ws.POST("/").
		To(func(request *restful.Request, response *restful.Response) {
			function := model.Function{}
			err := request.ReadEntity(&function)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			err = s.Manager.StartFunction(&function)
			if err != nil {
				if errors.Is(err, common.ErrorFunctionExists) {
					s.handleRestError(response.WriteError(http.StatusConflict, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("create a function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("createFunction").
		Reads(model.Function{}))

	deleteFunctionHandler := func(response *restful.Response, namespace, name string) {
		err := s.Manager.DeleteFunction(namespace, name)
		if err != nil {
			if errors.Is(err, common.ErrorFunctionNotFound) {
				s.handleRestError(response.WriteError(http.StatusNotFound, err))
				return
			}
			s.handleRestError(response.WriteError(http.StatusBadRequest, err))
			return
		}
		response.WriteHeader(http.StatusOK)
	}

	ws.Route(ws.DELETE("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			deleteFunctionHandler(response, "", name)
		}).
		Doc("delete a function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("deleteFunction").
		Param(ws.PathParameter("name", "name of the function").DataType("string")))

	ws.Route(ws.DELETE("/{namespace}/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			namespace := request.PathParameter("namespace")
			name := request.PathParameter("name")
			deleteFunctionHandler(response, namespace, name)
		}).
		Doc("delete a namespaced function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("deleteNamespacedFunction").
		Param(ws.PathParameter("name", "name of the function").DataType("string")).
		Param(ws.PathParameter("namespace", "namespace of the function").DataType("string")))

	return ws
}
