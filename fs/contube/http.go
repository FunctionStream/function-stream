/*
 * Copyright 2024 Function Stream Org.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package contube

import (
	"github.com/functionstream/function-stream/common"
	"io"
	"net/http"
	"sync"
	"sync/atomic"

	"github.com/pkg/errors"
	"golang.org/x/net/context"
)

type state int

const (
	EndpointKey     = "endpoint"
	IncludeMetadata = "includeMetadata"

	stateReady  state = iota // TODO: Why do we need this? Maybe we can remove it.
	stateClosed state = iota
)

var (
	ErrEndpointNotFound        = errors.New("endpoint not found")
	ErrEndpointClosed          = errors.New("endpoint closed")
	ErrorEndpointAlreadyExists = errors.New("endpoint already exists")
)

type EndpointHandler func(ctx context.Context, endpoint string, payload []byte) error
type HttpHandler func(w http.ResponseWriter, r *http.Request, payload []byte) Record

func DefaultHttpHandler(_ http.ResponseWriter, _ *http.Request, payload []byte) Record {
	return NewRecordImpl(payload, func() {})
}

type endpointHandler struct {
	ctx context.Context
	s   atomic.Value
	c   chan Record
}

type HttpTubeFactory struct {
	TubeFactory
	ctx       context.Context
	mu        sync.RWMutex
	endpoints map[string]*endpointHandler
	handler   HttpHandler
}

func NewHttpTubeFactory(ctx context.Context) *HttpTubeFactory {
	return NewHttpTubeFactoryWithIntercept(ctx, DefaultHttpHandler)
}

func NewHttpTubeFactoryWithIntercept(ctx context.Context, handler HttpHandler) *HttpTubeFactory {
	return &HttpTubeFactory{
		ctx:       ctx,
		endpoints: make(map[string]*endpointHandler),
		handler:   handler,
	}
}

type httpSourceTubeConfig struct {
	Endpoint string `json:"endpoint" validate:"required"`
}

func (f *HttpTubeFactory) getEndpoint(endpoint string) (*endpointHandler, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	e, ok := f.endpoints[endpoint]
	return e, ok
}

func (f *HttpTubeFactory) Handle(ctx context.Context, endpoint string, record Record) error {
	e, ok := f.getEndpoint(endpoint)
	if !ok {
		return ErrEndpointNotFound
	}
	if e.s.Load() == stateClosed {
		return ErrEndpointClosed
	}
	select {
	case e.c <- record:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return ErrEndpointClosed
	}
}

func (f *HttpTubeFactory) NewSourceTube(ctx context.Context, config ConfigMap) (<-chan Record, error) {
	c := httpSourceTubeConfig{}
	if err := config.ToConfigStruct(&c); err != nil {
		return nil, err
	}
	result := make(chan Record, 10)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.endpoints[c.Endpoint]; ok {
		return nil, ErrorEndpointAlreadyExists
	}
	var s atomic.Value
	s.Store(stateReady)
	handlerCtx, cancel := context.WithCancel(f.ctx)
	e := &endpointHandler{
		c:   result,
		s:   s,
		ctx: handlerCtx,
	}
	f.endpoints[c.Endpoint] = e
	go func() {
		<-ctx.Done()
		cancel()
		close(result)
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.endpoints, c.Endpoint)
	}()
	return result, nil
}

func (f *HttpTubeFactory) NewSinkTube(_ context.Context, _ ConfigMap) (chan<- Record, error) {
	return nil, ErrSinkTubeNotImplemented
}

func (f *HttpTubeFactory) GetHandleFunc(getEndpoint func(r *http.Request) (string, error),
	logger *common.Logger) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		endpoint, err := getEndpoint(r)
		if err != nil {
			logger.Error(err, "Failed to get endpoint")
			http.Error(w, errors.Wrap(err, "Failed to get endpoint").Error(), http.StatusBadRequest)
			return
		}
		log := logger.SubLogger("endpoint", endpoint, "component", "http-tube")
		if log.DebugEnabled() {
			log.Debug("Handle records from http request")
		}
		content, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error(err, "Failed to read body")
			http.Error(w, errors.Wrap(err, "Failed to read body").Error(), http.StatusBadRequest)
			return
		}
		record := f.handler(w, r, content)
		err = f.Handle(r.Context(), endpoint, record)
		if err != nil {
			log.Error(err, "Failed to handle record")
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if log.DebugEnabled() {
			log.Debug("Handled records from http request")
		}
	}
}
