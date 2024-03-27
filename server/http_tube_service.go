package server

import (
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"net/http"
)

func (s *Server) makeHttpTubeService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/http-tube")

	tags := []string{"http-tube"}

	ws.Route(ws.POST("/{endpoint}").
		To(func(request *restful.Request, response *restful.Response) {
			s.options.httpTubeFact.GetHandleFunc(func(r *http.Request) (string, error) {
				return request.PathParameter("name"), nil
			}, s.log)(response.ResponseWriter, request.Request)
		}).
		Doc("trigger the http tube endpoint").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("triggerHttpTubeEndpoint"))
	return ws
}
