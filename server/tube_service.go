package server

import (
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/contube"
	"io"
	"net/http"
)

func (s *Server) makeTubeService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1")

	tags := []string{"tube"}

	tubeName := ws.PathParameter("name", "tube name").DataType("string")

	ws.Route(ws.POST("/produce/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			body := request.Request.Body
			defer body.Close()

			content, err := io.ReadAll(body)
			if err != nil {
				response.WriteErrorString(http.StatusInternalServerError, err.Error())
				return
			}
			err = s.Manager.ProduceEvent(name, contube.NewRecordImpl(content, func() {}))
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("produce a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("produceMessage").
		Param(tubeName))

	ws.Route(ws.GET("/consume/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			record, err := s.Manager.ConsumeEvent(name)
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
			response.Write(record.GetPayload())
		}).
		Doc("consume a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("consumeMessage").
		Param(tubeName))

	return ws
}
