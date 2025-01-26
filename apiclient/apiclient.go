package apiclient

import (
	"bytes"
	"context"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"github.com/functionstream/function-stream/fs/model"
	"github.com/pkg/errors"
	"io"
	"mime/multipart"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
)

type Configuration struct {
	Host          string            `json:"host,omitempty"`
	Scheme        string            `json:"scheme,omitempty"`
	DefaultHeader map[string]string `json:"defaultHeader,omitempty"`
	UserAgent     string            `json:"userAgent,omitempty"`
	Debug         bool              `json:"debug,omitempty"`
	HTTPClient    *http.Client
}

func NewConfiguration() *Configuration {
	cfg := &Configuration{
		DefaultHeader: make(map[string]string),
		UserAgent:     "Function-Stream-API-Client", // TODO: Add version support
		Debug:         false,
		Scheme:        "http",
		Host:          "localhost:7300",
	}
	return cfg
}

type APIClient struct {
	cfg *Configuration
}

func NewAPIClient(cfg *Configuration) *APIClient {
	if cfg.HTTPClient == nil {
		cfg.HTTPClient = http.DefaultClient
	}

	return &APIClient{
		cfg: cfg,
	}
}

type formFile struct {
	fileBytes    []byte
	fileName     string
	formFileName string
}

// detectContentType method is used to figure out `Request.Body` content type for request header
func detectContentType(body interface{}) string {
	contentType := "text/plain; charset=utf-8"
	kind := reflect.TypeOf(body).Kind()

	switch kind {
	case reflect.Struct, reflect.Map, reflect.Ptr:
		contentType = "application/json; charset=utf-8"
	case reflect.String:
		contentType = "text/plain; charset=utf-8"
	default:
		if b, ok := body.([]byte); ok {
			contentType = http.DetectContentType(b)
		} else if kind == reflect.Slice {
			contentType = "application/json; charset=utf-8"
		}
	}

	return contentType
}

var (
	JsonCheck       = regexp.MustCompile(`(?i:(?:application|text)/(?:[^;]+\+)?json)`)
	XmlCheck        = regexp.MustCompile(`(?i:(?:application|text)/(?:[^;]+\+)?xml)`)
	queryParamSplit = regexp.MustCompile(`(^|&)([^&]+)`)
	queryDescape    = strings.NewReplacer("%5B", "[", "%5D", "]")
)

// Set request body from an interface{}
func setBody(body interface{}, contentType string) (bodyBuf *bytes.Buffer, err error) {
	if bodyBuf == nil {
		bodyBuf = &bytes.Buffer{}
	}

	if reader, ok := body.(io.Reader); ok {
		_, err = bodyBuf.ReadFrom(reader)
	} else if fp, ok := body.(*os.File); ok {
		_, err = bodyBuf.ReadFrom(fp)
	} else if b, ok := body.([]byte); ok {
		_, err = bodyBuf.Write(b)
	} else if s, ok := body.(string); ok {
		_, err = bodyBuf.WriteString(s)
	} else if s, ok := body.(*string); ok {
		_, err = bodyBuf.WriteString(*s)
	} else if JsonCheck.MatchString(contentType) {
		err = json.NewEncoder(bodyBuf).Encode(body)
	} else if XmlCheck.MatchString(contentType) {
		var bs []byte
		bs, err = xml.Marshal(body)
		if err == nil {
			bodyBuf.Write(bs)
		}
	}

	if err != nil {
		return nil, err
	}

	if bodyBuf.Len() == 0 {
		err = fmt.Errorf("invalid body type %s\n", contentType)
		return nil, err
	}
	return bodyBuf, nil
}

// Add a file to the multipart request
func addFile(w *multipart.Writer, fieldName, path string) error {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return err
	}
	err = file.Close()
	if err != nil {
		return err
	}

	part, err := w.CreateFormFile(fieldName, filepath.Base(path))
	if err != nil {
		return err
	}
	_, err = io.Copy(part, file)

	return err
}

// prepareRequest build the request
func (c *APIClient) prepareRequest(
	ctx context.Context,
	path string, method string,
	postBody interface{},
	headerParams map[string]string,
	queryParams url.Values,
	formParams url.Values,
	formFiles []formFile) (localVarRequest *http.Request, err error) {

	var body *bytes.Buffer

	if headerParams == nil {
		headerParams = make(map[string]string)
	}

	// Detect postBody type and post.
	if postBody != nil {
		contentType := headerParams["Content-Type"]
		if contentType == "" {
			contentType = detectContentType(postBody)
			headerParams["Content-Type"] = contentType
		}

		body, err = setBody(postBody, contentType)
		if err != nil {
			return nil, err
		}
	}

	// add form parameters and file if available.
	if strings.HasPrefix(headerParams["Content-Type"], "multipart/form-data") && len(formParams) > 0 || (len(formFiles) > 0) {
		if body != nil {
			return nil, errors.New("Cannot specify postBody and multipart form at the same time.")
		}
		body = &bytes.Buffer{}
		w := multipart.NewWriter(body)

		for k, v := range formParams {
			for _, iv := range v {
				if strings.HasPrefix(k, "@") { // file
					err = addFile(w, k[1:], iv)
					if err != nil {
						return nil, err
					}
				} else { // form value
					w.WriteField(k, iv)
				}
			}
		}
		for _, formFile := range formFiles {
			if len(formFile.fileBytes) > 0 && formFile.fileName != "" {
				w.Boundary()
				part, err := w.CreateFormFile(formFile.formFileName, filepath.Base(formFile.fileName))
				if err != nil {
					return nil, err
				}
				_, err = part.Write(formFile.fileBytes)
				if err != nil {
					return nil, err
				}
			}
		}

		// Set the Boundary in the Content-Type
		headerParams["Content-Type"] = w.FormDataContentType()

		// Set Content-Length
		headerParams["Content-Length"] = fmt.Sprintf("%d", body.Len())
		w.Close()
	}

	if strings.HasPrefix(headerParams["Content-Type"], "application/x-www-form-urlencoded") && len(formParams) > 0 {
		if body != nil {
			return nil, errors.New("Cannot specify postBody and x-www-form-urlencoded form at the same time.")
		}
		body = &bytes.Buffer{}
		body.WriteString(formParams.Encode())
		// Set Content-Length
		headerParams["Content-Length"] = fmt.Sprintf("%d", body.Len())
	}

	// Setup path and query parameters
	url, err := url.Parse(path)
	if err != nil {
		return nil, err
	}

	// Override request host, if applicable
	if c.cfg.Host != "" {
		url.Host = c.cfg.Host
	}

	// Override request scheme, if applicable
	if c.cfg.Scheme != "" {
		url.Scheme = c.cfg.Scheme
	}

	// Adding Query Param
	query := url.Query()
	for k, v := range queryParams {
		for _, iv := range v {
			query.Add(k, iv)
		}
	}

	// Encode the parameters.
	url.RawQuery = queryParamSplit.ReplaceAllStringFunc(query.Encode(), func(s string) string {
		pieces := strings.Split(s, "=")
		pieces[0] = queryDescape.Replace(pieces[0])
		return strings.Join(pieces, "=")
	})

	// Generate a new request
	if body != nil {
		localVarRequest, err = http.NewRequest(method, url.String(), body)
	} else {
		localVarRequest, err = http.NewRequest(method, url.String(), nil)
	}
	if err != nil {
		return nil, err
	}

	// add header parameters, if any
	if len(headerParams) > 0 {
		headers := http.Header{}
		for h, v := range headerParams {
			headers[h] = []string{v}
		}
		localVarRequest.Header = headers
	}

	// Add the user agent to the request.
	localVarRequest.Header.Add("User-Agent", c.cfg.UserAgent)

	if ctx != nil {
		// add context to the request
		localVarRequest = localVarRequest.WithContext(ctx)

		// Walk through any authentication.

	}

	for header, value := range c.cfg.DefaultHeader {
		localVarRequest.Header.Add(header, value)
	}
	return localVarRequest, nil
}

// callAPI do the request.
func (c *APIClient) callAPI(request *http.Request) (*http.Response, error) {
	if c.cfg.Debug {
		dump, err := httputil.DumpRequestOut(request, true)
		if err != nil {
			return nil, err
		}
		fmt.Printf("\n%s\n", string(dump))
	}

	resp, err := c.cfg.HTTPClient.Do(request)
	if err != nil {
		return resp, err
	}

	if c.cfg.Debug {
		dump, err := httputil.DumpResponse(resp, true)
		if err != nil {
			return resp, err
		}
		fmt.Printf("\n%s\n", string(dump))
	}
	return resp, err
}

func (c *APIClient) HandleResponse(resp *http.Response, e error) ([]byte, error) {
	bodyString := ""
	var bodyBytes []byte
	if resp != nil {
		var err error
		bodyBytes, err = io.ReadAll(resp.Body)
		if err == nil {
			bodyString = string(bodyBytes)
		} else {
			return nil, fmt.Errorf("failed to read response body: %v", err)
		}
	}

	if e != nil {
		var netErr *net.OpError
		if errors.As(e, &netErr) {
			var sysErr *os.SyscallError
			if errors.As(e, &sysErr) {
				return nil, fmt.Errorf("failed to connect to the server: %s", netErr.Addr)
			}
			return nil, fmt.Errorf("request error: [%v] %s", netErr, bodyString)
		}
		return nil, fmt.Errorf("request error: [%v] %s", e, bodyString)
	}
	if resp == nil {
		return nil, fmt.Errorf("empty response")
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("HTTP %d: %s", resp.StatusCode, bodyString)
	}

	return bodyBytes, nil
}

type Response struct {
	HttpResponse *http.Response
}

func (c *APIClient) FunctionService() *FunctionService {
	return &FunctionService{
		client: c,
	}
}

type FunctionService struct {
	client *APIClient
}

func (fs *FunctionService) Deploy(ctx context.Context, f *model.Function) error {
	req, err := fs.client.prepareRequest(ctx, "/apis/v1/functions", http.MethodPost, f, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = fs.client.HandleResponse(fs.client.callAPI(req))
	return err
}

func (fs *FunctionService) List(ctx context.Context) ([]*model.Function, error) {
	req, err := fs.client.prepareRequest(ctx, "/apis/v1/functions", http.MethodGet, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := fs.client.HandleResponse(fs.client.callAPI(req))
	if err != nil {
		return nil, err
	}
	var functions []*model.Function
	err = json.Unmarshal(resp, &functions)
	if err != nil {
		return nil, err
	}
	return functions, nil
}

func (fs *FunctionService) Delete(ctx context.Context, name string) error {
	req, err := fs.client.prepareRequest(ctx, "/apis/v1/functions/"+name, http.MethodDelete, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = fs.client.HandleResponse(fs.client.callAPI(req))
	return err
}

type PackageService struct {
	client *APIClient
}

func (c *APIClient) PackageService() *PackageService {
	return &PackageService{
		client: c,
	}
}

func (ps *PackageService) Create(ctx context.Context, pkg *model.Package) error {
	req, err := ps.client.prepareRequest(ctx, "/apis/v1/packages", http.MethodPost, pkg, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = ps.client.HandleResponse(ps.client.callAPI(req))
	return err
}

func (ps *PackageService) List(ctx context.Context) ([]*model.Package, error) {
	req, err := ps.client.prepareRequest(ctx, "/apis/v1/packages", http.MethodGet, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := ps.client.HandleResponse(ps.client.callAPI(req))
	if err != nil {
		return nil, err
	}
	var packages []*model.Package
	err = json.Unmarshal(resp, &packages)
	if err != nil {
		return nil, err
	}
	return packages, nil
}

func (ps *PackageService) Read(ctx context.Context, name string) (*model.Package, error) {
	req, err := ps.client.prepareRequest(ctx, "/apis/v1/packages/"+name, http.MethodGet, nil, nil, nil, nil, nil)
	if err != nil {
		return nil, err
	}
	resp, err := ps.client.HandleResponse(ps.client.callAPI(req))
	if err != nil {
		return nil, err
	}
	var pkg model.Package
	err = json.Unmarshal(resp, &pkg)
	if err != nil {
		return nil, err
	}
	return &pkg, nil
}

func (ps *PackageService) Update(ctx context.Context, pkg *model.Package) error {
	req, err := ps.client.prepareRequest(ctx, "/apis/v1/packages/"+pkg.Name, http.MethodPut, pkg, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = ps.client.HandleResponse(ps.client.callAPI(req))
	return err
}

func (ps *PackageService) Delete(ctx context.Context, name string) error {
	req, err := ps.client.prepareRequest(ctx, "/apis/v1/packages/"+name, http.MethodDelete, nil, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = ps.client.HandleResponse(ps.client.callAPI(req))
	return err
}

type EventsService struct {
	client *APIClient
}

func (c *APIClient) EventsService() *EventsService {
	return &EventsService{
		client: c,
	}
}

type MappedNullable interface {
	ToMap() (map[string]interface{}, error)
}

func parameterValueToString(obj interface{}, key string) string {
	if reflect.TypeOf(obj).Kind() != reflect.Ptr {
		return fmt.Sprintf("%v", obj)
	}
	var param, ok = obj.(MappedNullable)
	if !ok {
		return ""
	}
	dataMap, err := param.ToMap()
	if err != nil {
		return ""
	}
	return fmt.Sprintf("%v", dataMap[key])
}

func (es *EventsService) Consume(ctx context.Context, topic string) (string, error) {
	localVarPath := "/api/v1/events/consume/{name}"
	localVarPath = strings.Replace(localVarPath, "{"+"name"+"}", url.PathEscape(parameterValueToString(topic, "name")), -1)

	req, err := es.client.prepareRequest(ctx, localVarPath, http.MethodGet, nil, nil, nil, nil, nil)
	if err != nil {
		return "", err
	}
	resp, err := es.client.HandleResponse(es.client.callAPI(req))
	if err != nil {
		return "", err
	}
	return string(resp), nil
}

func (es *EventsService) Produce(ctx context.Context, topic string, event any) error {
	localVarPath := "/api/v1/events/produce/{name}"
	localVarPath = strings.Replace(localVarPath, "{"+"name"+"}", url.PathEscape(parameterValueToString(topic, "name")), -1)

	req, err := es.client.prepareRequest(ctx, localVarPath, http.MethodPost, event, nil, nil, nil, nil)
	if err != nil {
		return err
	}
	_, err = es.client.HandleResponse(es.client.callAPI(req))
	return err
}
