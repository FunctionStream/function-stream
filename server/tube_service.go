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
	"github.com/functionstream/function-stream/fs/contube"
	"io"
	"net/http"
)

// Due to this issue: https://github.com/emicklei/go-restful-openapi/issues/115,
// we need to use this schema to specify the format of the byte array.
var bytesSchema = restfulspec.SchemaType{RawType: "string", Format: "byte"}

func (s *Server) makeTubeService() *restful.WebService {

	ws := new(restful.WebService)
	ws.Path("/api/v1").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"tube"}

	tubeName := ws.PathParameter("name", "tube name").DataType("string")

	ws.Route(ws.POST("/produce/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			body := request.Request.Body
			defer func() {
				s.handleRestError(body.Close())
			}()

			content, err := io.ReadAll(body)
			if err != nil {
				s.handleRestError(response.WriteErrorString(http.StatusInternalServerError, err.Error()))
				return
			}
			err = s.Manager.ProduceEvent(name, contube.NewRecordImpl(content, func() {}))
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("produce a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("produceMessage").
		Reads(bytesSchema).
		Param(tubeName))

	ws.Route(ws.GET("/consume/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			record, err := s.Manager.ConsumeEvent(name)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}
			_, err = response.Write(record.GetPayload())
			s.handleRestError(err)
		}).
		Doc("consume a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("consumeMessage").
		Writes(bytesSchema).
		Returns(http.StatusOK, "OK", bytesSchema).
		Param(tubeName))

	return ws
}
