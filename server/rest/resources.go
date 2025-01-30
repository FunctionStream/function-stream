package rest

import (
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/pkg/errors"
	"go.uber.org/zap"
	"net/http"
	"strings"
)

type ResourceServiceBuilder[T any] struct {
	name             string
	basePath         string
	resourceProvider api.ResourceProvider[T]
}

func (b *ResourceServiceBuilder[T]) Name(name string) *ResourceServiceBuilder[T] {
	b.name = name
	return b
}

func (b *ResourceServiceBuilder[T]) ResourceProvider(resourceProvider api.ResourceProvider[T]) *ResourceServiceBuilder[T] {
	b.resourceProvider = resourceProvider
	return b
}

func capitalizeFirstLetter(s string) string {
	if len(s) == 0 {
		return s
	}
	return strings.ToUpper(string(s[0])) + s[1:]
}

func (b *ResourceServiceBuilder[T]) Build(basePath string, log *zap.Logger) *restful.WebService {
	ws := &restful.WebService{}
	ws.Path(basePath + b.name).Consumes(restful.MIME_JSON).Produces(restful.MIME_JSON)

	tags := []string{b.name}
	r := b.resourceProvider

	l := log.With(zap.String("resource", b.name))

	handleRestError := func(e error) {
		if e == nil {
			return
		}
		l.Error("Error handling REST request", zap.Error(e))
	}

	ws.Route(ws.GET("/").
		To(func(request *restful.Request, response *restful.Response) {
			packages, err := r.List(request.Request.Context())
			if err != nil {
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			handleRestError(response.WriteEntity(packages))
		}).
		Doc("get all "+b.name).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getAll"+capitalizeFirstLetter(b.name)).
		Returns(http.StatusOK, "OK", []*T{}).
		Writes([]*T{}))

	ws.Route(ws.GET("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			t, err := r.Read(request.Request.Context(), name)
			if err != nil {
				if errors.Is(err, api.ErrResourceNotFound) {
					handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			handleRestError(response.WriteEntity(t))
		}).
		Doc("get "+b.name).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("get"+capitalizeFirstLetter(b.name)).
		Param(ws.PathParameter("name", "name of the "+b.name).DataType("string")).
		Returns(http.StatusOK, "OK", new(T)).
		Writes(new(T)))

	ws.Route(ws.POST("/").
		To(func(request *restful.Request, response *restful.Response) {
			t := new(T)
			err := request.ReadEntity(t)
			if err != nil {
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			err = r.Create(request.Request.Context(), t)
			if err != nil {
				if errors.Is(err, api.ErrResourceAlreadyExists) {
					handleRestError(response.WriteError(http.StatusConflict, err))
					return
				}
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("create "+b.name).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("create" + capitalizeFirstLetter(b.name)).
		Reads(new(T)))

	ws.Route(ws.PUT("/").
		To(func(request *restful.Request, response *restful.Response) {
			t := new(T)
			err := request.ReadEntity(t)
			if err != nil {
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			err = r.Upsert(request.Request.Context(), t)
			if err != nil {
				if errors.Is(err, api.ErrResourceNotFound) {
					handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("update "+b.name).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("update" + capitalizeFirstLetter(b.name)).
		Reads(new(T)))

	ws.Route(ws.DELETE("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			if err := r.Delete(request.Request.Context(), name); err != nil {
				if errors.Is(err, api.ErrResourceNotFound) {
					handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
		}).
		Doc("delete "+b.name).
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("delete" + capitalizeFirstLetter(b.name)).
		Param(ws.PathParameter("name", "name of the package").DataType("string")))

	l.Info("Resource service setup complete", zap.String("name", b.name), zap.String("apiPath", basePath+b.name))

	return ws
}
