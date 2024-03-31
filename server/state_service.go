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
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/pkg/errors"
	"io"
	"net/http"
)

func (s *Server) makeStateService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/state")

	tags := []string{"state"}

	keyParam := ws.PathParameter("key", "state key").DataType("string")

	ws.Route(ws.POST("/{key}").
		To(func(request *restful.Request, response *restful.Response) {
			key := request.PathParameter("key")

			state := s.Manager.GetStateStore()
			if state == nil {
				s.handleRestError(response.WriteErrorString(http.StatusBadRequest, "No state store configured"))
				return
			}

			body := request.Request.Body
			defer func() {
				s.handleRestError(body.Close())
			}()

			content, err := io.ReadAll(body)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusBadRequest, errors.Wrap(err, "Failed to read body")))
				return
			}

			err = state.PutState(key, content)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}
		}).
		Doc("set a state").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("setState").
		Param(keyParam).
		Reads(bytesSchema))

	ws.Route(ws.GET("/{key}").
		To(func(request *restful.Request, response *restful.Response) {
			key := request.PathParameter("key")
			state := s.Manager.GetStateStore()
			if state == nil {
				s.handleRestError(response.WriteErrorString(http.StatusBadRequest, "No state store configured"))
				return
			}

			content, err := state.GetState(key)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}

			_, err = response.Write(content)
			s.handleRestError(err)
		}).
		Doc("get a state").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getState").
		Writes(bytesSchema).
		Returns(http.StatusOK, "OK", bytesSchema).
		Param(keyParam))

	return ws
}
