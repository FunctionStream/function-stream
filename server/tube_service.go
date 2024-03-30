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
		Reads(bytesSchema).
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
		Writes(bytesSchema).
		Returns(http.StatusOK, "OK", bytesSchema).
		Param(tubeName))

	return ws
}
