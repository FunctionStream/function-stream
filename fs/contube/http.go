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
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"sync/atomic"
)

type state int

const (
	EndpointKey = "endpoint"

	stateReady  state = iota
	stateClosed state = iota
)

var (
	ErrEndpointNotFound        = errors.New("endpoint not found")
	ErrEndpointClosed          = errors.New("endpoint closed")
	ErrorEndpointAlreadyExists = errors.New("endpoint already exists")
)

type EndpointHandler func(ctx context.Context, endpoint string, payload []byte) error

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
}

func NewHttpTubeFactory(ctx context.Context) *HttpTubeFactory {
	return &HttpTubeFactory{
		ctx:       ctx,
		endpoints: make(map[string]*endpointHandler),
	}
}

type httpSourceTubeConfig struct {
	endpoint string
}

func (c ConfigMap) toHttpSourceTubeConfig() (*httpSourceTubeConfig, error) {
	endpoint, ok := c[EndpointKey].(string)
	if !ok {
		return nil, ErrEndpointNotFound
	}
	return &httpSourceTubeConfig{
		endpoint: endpoint,
	}, nil
}

func (f *HttpTubeFactory) Handle(ctx context.Context, endpoint string, payload []byte) error {
	f.mu.RLock()
	e, ok := f.endpoints[endpoint]
	if !ok {
		f.mu.RUnlock()
		return ErrEndpointNotFound
	}
	f.mu.RUnlock()
	if e.s.Load() == stateClosed {
		return ErrEndpointClosed
	}
	select {
	case e.c <- NewRecordImpl(payload, func() {}):
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-e.ctx.Done():
		return ErrEndpointClosed
	}
}

func (f *HttpTubeFactory) NewSourceTube(ctx context.Context, config ConfigMap) (<-chan Record, error) {
	c, err := config.toHttpSourceTubeConfig()
	if err != nil {
		return nil, err
	}
	result := make(chan Record, 10)
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.endpoints[c.endpoint]; ok {
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
	f.endpoints[c.endpoint] = e
	go func() {
		<-ctx.Done()
		cancel()
		close(result)
		f.mu.Lock()
		defer f.mu.Unlock()
		delete(f.endpoints, c.endpoint)
	}()
	return result, nil
}

func (f *HttpTubeFactory) NewSinkTube(_ context.Context, _ ConfigMap) (chan<- Record, error) {
	return nil, ErrSinkTubeNotImplemented
}

func (f *HttpTubeFactory) GetHandleFunc(getEndpoint func(r *http.Request) (string, error), logger *slog.Logger) func(http.ResponseWriter, *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		endpoint, err := getEndpoint(r)
		if err != nil {
			logger.Error("Failed to get endpoint", "error", err)
			http.Error(w, errors.Wrap(err, "Failed to get endpoint").Error(), http.StatusBadRequest)
			return
		}
		log := logger.With(slog.String("endpoint", endpoint), slog.String("component", "http-tube"))
		log.Info("Handle records from http request")
		content, err := io.ReadAll(r.Body)
		if err != nil {
			log.Error("Failed to read body", "error", err)
			http.Error(w, errors.Wrap(err, "Failed to read body").Error(), http.StatusBadRequest)
			return
		}
		err = f.Handle(r.Context(), endpoint, content)
		if err != nil {
			log.Error("Failed to handle record", "error", err)
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		log.Info("Handled records from http request")
	}
}
