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
	"net/http"

	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
)

func (s *Server) makeHttpTubeService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/http-tube").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"http-tube"}

	ws.Route(ws.POST("/{endpoint}").
		To(func(request *restful.Request, response *restful.Response) {
			s.options.httpTubeFact.GetHandleFunc(func(r *http.Request) (string, error) {
				return request.PathParameter("endpoint"), nil
			}, s.log)(response.ResponseWriter, request.Request)
		}).
		Doc("trigger the http tube endpoint").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Param(ws.PathParameter("endpoint", "Endpoint").DataType("string")).
		Reads(bytesSchema).
		Operation("triggerHttpTubeEndpoint"))
	return ws
}
