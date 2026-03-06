// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package impl

import (
	"strings"
	"sync"

	"github.com/functionstream/function-stream/go-sdk/api"
	"github.com/functionstream/function-stream/go-sdk/bindings/functionstream/core/collector"
)

type runtimeContext struct {
	mu     sync.RWMutex
	config map[string]string
	stores map[string]*storeImpl
	closed bool
}

func newRuntimeContext(config map[string]string) *runtimeContext {
	return &runtimeContext{
		config: cloneStringMap(config),
		stores: make(map[string]*storeImpl),
	}
}

func (c *runtimeContext) Emit(targetID uint32, data []byte) error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return api.NewError(api.ErrRuntimeClosed, "emit on closed context")
	}
	collector.Emit(targetID, toList(data))
	return nil
}

func (c *runtimeContext) EmitWatermark(targetID uint32, watermark uint64) error {
	c.mu.RLock()
	closed := c.closed
	c.mu.RUnlock()
	if closed {
		return api.NewError(api.ErrRuntimeClosed, "emit watermark on closed context")
	}
	collector.EmitWatermark(targetID, watermark)
	return nil
}

func (c *runtimeContext) GetOrCreateStore(name string) (api.Store, error) {
	storeName := strings.TrimSpace(name)
	if storeName == "" {
		return nil, api.NewError(api.ErrStoreInvalidName, "store name must not be empty")
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return nil, api.NewError(api.ErrRuntimeClosed, "store request on closed context")
	}
	if existing, ok := c.stores[storeName]; ok {
		return existing, nil
	}

	store := newStore(storeName)
	c.stores[storeName] = store
	return store, nil
}

func (c *runtimeContext) Config() map[string]string {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return cloneStringMap(c.config)
}

func (c *runtimeContext) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	stores := c.stores
	c.stores = make(map[string]*storeImpl)
	c.mu.Unlock()

	var firstErr error
	for _, store := range stores {
		if err := store.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	return firstErr
}

func cloneStringMap(input map[string]string) map[string]string {
	out := make(map[string]string, len(input))
	for k, v := range input {
		out[k] = v
	}
	return out
}

func cloneBytes(input []byte) []byte {
	if input == nil {
		return nil
	}
	out := make([]byte, len(input))
	copy(out, input)
	return out
}
