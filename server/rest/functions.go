package rest

import (
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/pkg/errors"
	"net/http"
)

func (s *Handler) makeFunctionsService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/apis/v1/functions").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"functions"}

	ws.Route(ws.GET("/").
		To(func(request *restful.Request, response *restful.Response) {
			functions := s.Manager.List()
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
			err = s.Manager.Deploy(request.Request.Context(), &function)
			if err != nil {
				if errors.Is(err, api.ErrFunctionAlreadyExists) {
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

	ws.Route(ws.DELETE("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			if err := s.Manager.Delete(request.Request.Context(), name); err != nil {
				if errors.Is(err, api.ErrFunctionNotFound) {
					s.handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
		}).
		Doc("delete a function").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("deleteFunction").
		Param(ws.PathParameter("name", "name of the function").DataType("string")))

	return ws
}
