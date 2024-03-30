package server

import (
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/common/model"
	"net/http"
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
			response.WriteEntity(functions)
		}).
		Doc("get all functions").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getAllFunctions").
		Returns(http.StatusOK, "OK", []model.Function{}).
		Writes([]model.Function{}))

	ws.Route(ws.POST("/").
		To(func(request *restful.Request, response *restful.Response) {
			function := model.Function{}
			err := request.ReadEntity(&function)
			if err != nil {
				response.WriteError(http.StatusBadRequest, err)
				return
			}
			err = s.Manager.StartFunction(&function)
			if err != nil {
				response.WriteError(http.StatusBadRequest, err)
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("create a function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("createFunction").
		Reads(model.Function{}))

	ws.Route(ws.DELETE("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			err := s.Manager.DeleteFunction(name)
			if err != nil {
				response.WriteError(http.StatusBadRequest, err)
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("delete a function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("deleteFunction").
		Param(ws.PathParameter("name", "name of the function").DataType("string")))

	return ws
}
