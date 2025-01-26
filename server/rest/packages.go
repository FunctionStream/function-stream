package rest

import (
	"errors"
	"fmt"
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"net/http"
)

func (s *Handler) makePackagesService() *restful.WebService {
	ws := new(restful.WebService)
	ws.Path("/apis/v1/packages").
		Consumes(restful.MIME_JSON).
		Produces(restful.MIME_JSON)

	tags := []string{"packages"}

	ws.Route(ws.GET("/").
		To(func(request *restful.Request, response *restful.Response) {
			packages, err := s.PackageStorage.List(request.Request.Context())
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			s.handleRestError(response.WriteEntity(packages))
		}).
		Doc("get all packages").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getAllPackages").
		Returns(http.StatusOK, "OK", []*model.Package{}).
		Writes([]*model.Package{}))

	ws.Route(ws.GET("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			pkg, err := s.PackageStorage.Read(request.Request.Context(), name)
			if err != nil {
				if errors.Is(err, api.ErrPackageNotFound) {
					s.handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			s.handleRestError(response.WriteEntity(pkg))
		}).
		Doc("get a package").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("getPackage").
		Param(ws.PathParameter("name", "name of the package").DataType("string")).
		Returns(http.StatusOK, "OK", model.Package{}).
		Writes(model.Package{}))

	ws.Route(ws.POST("/").
		To(func(request *restful.Request, response *restful.Response) {
			pkg := model.Package{}
			err := request.ReadEntity(&pkg)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			err = s.PackageStorage.Create(request.Request.Context(), &pkg)
			if err != nil {
				if errors.Is(err, api.ErrPackageAlreadyExists) {
					s.handleRestError(response.WriteError(http.StatusConflict, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("create a package").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("createPackage").
		Reads(model.Package{}))

	ws.Route(ws.PUT("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			pkg := model.Package{}
			err := request.ReadEntity(&pkg)
			if err != nil {
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			if name != pkg.Name {
				s.handleRestError(response.WriteError(http.StatusBadRequest, fmt.Errorf("package cannot be changed")))
				return
			}
			err = s.PackageStorage.Update(request.Request.Context(), &pkg)
			if err != nil {
				if errors.Is(err, api.ErrPackageNotFound) {
					s.handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
			response.WriteHeader(http.StatusOK)
		}).
		Doc("update a package").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("updatePackage").
		Param(ws.PathParameter("name", "name of the package").DataType("string")).
		Reads(model.Package{}))

	ws.Route(ws.DELETE("/{name}").
		To(func(request *restful.Request, response *restful.Response) {
			name := request.PathParameter("name")
			if err := s.PackageStorage.Delete(request.Request.Context(), name); err != nil {
				if errors.Is(err, api.ErrPackageNotFound) {
					s.handleRestError(response.WriteError(http.StatusNotFound, err))
					return
				}
				s.handleRestError(response.WriteError(http.StatusBadRequest, err))
				return
			}
		}).
		Doc("delete a package").
		Metadata(restfulspec.KeyOpenAPITags, tags).
		Operation("deletePackage").
		Param(ws.PathParameter("name", "name of the package").DataType("string")))

	return ws
}
