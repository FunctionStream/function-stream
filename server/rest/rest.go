package rest

import (
	"context"
	restfulspec "github.com/emicklei/go-restful-openapi/v2"
	"github.com/emicklei/go-restful/v3"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/go-openapi/spec"
	"go.uber.org/zap"
	"net"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type Handler struct {
	HttpListener   net.Listener
	EnableTls      bool
	TlsCertFile    string
	TlsKeyFile     string
	Manager        api.Manager
	PackageStorage api.PackageStorage
	EventStorage   api.EventStorage
	Log            *zap.Logger

	httpSvr atomic.Pointer[http.Server]
}

func enrichSwaggerObject(swo *spec.Swagger) {
	swo.Info = &spec.Info{
		InfoProps: spec.InfoProps{
			Title:       "Function Stream Service",
			Description: "Manage Function Stream Resources",
			Contact: &spec.ContactInfo{
				ContactInfoProps: spec.ContactInfoProps{
					Name: "Function Stream Org",
					URL:  "https://github.com/FunctionStream",
				},
			},
			License: &spec.License{
				LicenseProps: spec.LicenseProps{
					Name: "Apache 2",
					URL:  "http://www.apache.org/licenses/",
				},
			},
			Version: "1.0.0", // TODO: Version control
		},
	}
	swo.Host = "localhost:7300"
	swo.Schemes = []string{"http"}
	swo.Tags = []spec.Tag{
		{
			TagProps: spec.TagProps{
				Name:        "functions",
				Description: "Managing functions"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "packages",
				Description: "Managing packages"},
		},
		{
			TagProps: spec.TagProps{
				Name:        "events",
				Description: "Producing and consuming events"},
		},
	}
}

func (h *Handler) Run() error {
	statusSvr := new(restful.WebService)
	statusSvr.Path("/api/v1/status").
		Route(statusSvr.GET("/").To(func(request *restful.Request, response *restful.Response) {
			response.WriteHeader(http.StatusOK)
		}).
			Doc("Get the status of the Function Stream").
			Metadata(restfulspec.KeyOpenAPITags, []string{"status"}).
			Operation("getStatus"))

	container := restful.NewContainer()

	cors := restful.CrossOriginResourceSharing{
		AllowedHeaders: []string{"Content-Type", "Accept"},
		AllowedMethods: []string{"GET", "POST", "OPTIONS", "PUT", "DELETE"},
		CookiesAllowed: false,
		Container:      container}
	container.Filter(cors.Filter)
	container.Filter(container.OPTIONSFilter)

	config := restfulspec.Config{
		WebServices:                   container.RegisteredWebServices(),
		APIPath:                       "/apidocs",
		PostBuildSwaggerObjectHandler: enrichSwaggerObject}
	container.Add(restfulspec.NewOpenAPIService(config))
	container.Add(h.makeEventsService())
	basePath := "/apis/v1/"
	container.Add(new(ResourceServiceBuilder[model.Package]).
		Name("package").
		ResourceProvider(h.PackageStorage).
		Build(basePath, h.Log))
	container.Add(new(ResourceServiceBuilder[model.Function]).
		Name("function").
		ResourceProvider(h.Manager).
		Build(basePath, h.Log))

	httpSvr := &http.Server{
		Handler: container.ServeMux,
	}
	h.httpSvr.Store(httpSvr)

	if h.EnableTls {
		return httpSvr.ServeTLS(h.HttpListener, h.TlsCertFile, h.TlsKeyFile)
	} else {
		return httpSvr.Serve(h.HttpListener)
	}
}

func (s *Handler) WaitForReady(ctx context.Context) <-chan struct{} {
	c := make(chan struct{})
	detect := func() bool {
		u := (&url.URL{
			Scheme: "http",
			Host:   s.HttpListener.Addr().String(),
			Path:   "/api/v1/status",
		}).String()
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			s.Log.Error("Failed to create detect request", zap.Error(err))
			return false
		}
		client := &http.Client{}
		_, err = client.Do(req)
		if err != nil {
			s.Log.Error("Detect connection to server failed", zap.Error(err))
			return false
		}
		s.Log.Info("Server is ready", zap.String("url", u))
		return true
	}
	go func() {
		defer close(c)

		if detect() {
			return
		}
		// Try to connect to the server
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(1 * time.Second):
				if detect() {
					return
				}
			}
		}
	}()
	return c
}

func (h *Handler) handleRestError(e error) {
	if e == nil {
		return
	}
	h.Log.Error("Error handling REST request", zap.Error(e))
}

func (h *Handler) Stop() error {
	h.Log.Info("Stopping REST handler")
	httpSvr := h.httpSvr.Load()
	if httpSvr != nil {
		return httpSvr.Close()
	}
	return nil
}
