package rest

import (
	"bytes"
	"encoding/json"
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/event"
	"github.com/functionstream/function-stream/fs/model"
	"io"
	"net/http"
)

func (h *Handler) makeEventsService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/api/v1/events").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"events"}

	eventName := ws.PathParameter("name", "topic name").DataType("string")

	ws.Route(ws.POST("/produce/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			body := request.Request.Body
			defer func() {
				h.handleRestError(body.Close())
			}()

			data, err := io.ReadAll(body)
			if err != nil {
				h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}

			e := event.NewRawEvent("", bytes.NewReader(data))
			if err := h.EventStorage.Write(request.Request.Context(), e, model.TopicConfig{
				Name: name,
			}); err != nil {
				h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}
			response.WriteHeader(http.StatusOK)
			//select {
			//case <-request.Request.Context().Done():
			//	h.handleRestError(response.WriteError(http.StatusRequestTimeout, request.Request.Context().Err()))
			//	return
			//case <-e.OnCommit():
			//	response.WriteHeader(http.StatusOK)
			//	return
			//}
		}).
		Doc("produce a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("produceMessage").
		Reads(map[string]any{}).
		Param(eventName))

	ws.Route(ws.GET("/consume/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			eCh, err := h.EventStorage.Read(request.Request.Context(), []model.TopicConfig{
				{
					Name: name,
				},
			})
			if err != nil {
				h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
				return
			}

			response.AddHeader("Transfer-Encoding", "chunked")
			for {
				select {
				case <-request.Request.Context().Done():
					h.handleRestError(response.WriteError(http.StatusRequestTimeout, request.Request.Context().Err()))
					return
				case e, ok := <-eCh:
					if !ok {
						return
					}
					data, err := io.ReadAll(e.Payload())
					if err != nil {
						h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
						return
					}
					entity := make(map[string]any)
					if err := json.Unmarshal(data, &entity); err != nil {
						h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
						return
					}
					if _, err := response.Write(data); err != nil {
						h.handleRestError(response.WriteError(http.StatusInternalServerError, err))
						return
					}
					response.Write([]byte("\n"))
					response.Flush()
				}
			}

		}).
		Doc("consume a message").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("consumeMessage").
		Writes(map[string]any{}).
		Param(eventName))

	return ws
}
