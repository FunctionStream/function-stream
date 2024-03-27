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
				response.WriteErrorString(http.StatusBadRequest, "No state store configured")
				return
			}

			body := request.Request.Body
			defer body.Close()

			content, err := io.ReadAll(body)
			if err != nil {
				response.WriteError(http.StatusBadRequest, errors.Wrap(err, "Failed to read body"))
				return
			}

			err = state.PutState(key, content)
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}
		}).
		Doc("set a state").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("setState").
		Param(keyParam))

	ws.Route(ws.GET("/{key}").
		To(func(request *restful.Request, response *restful.Response) {
			key := request.PathParameter("key")
			state := s.Manager.GetStateStore()
			if state == nil {
				response.WriteErrorString(http.StatusBadRequest, "No state store configured")
				return
			}

			content, err := state.GetState(key)
			if err != nil {
				response.WriteError(http.StatusInternalServerError, err)
				return
			}

			response.Write(content)
		}).
		Doc("get a state").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getState").
		Param(keyParam))

	return ws
}
