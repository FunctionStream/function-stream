/*
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

package lib

import (
	"context"
	"sync"
)

type MemoryQueueFactory struct {
	mu     sync.Mutex
	queues map[string]chan Event
}

func NewMemoryQueueFactory() EventQueueFactory {
	return &MemoryQueueFactory{
		queues: make(map[string]chan Event),
	}
}

func (f *MemoryQueueFactory) getOrCreateChan(name string) chan Event {
	if queue, ok := f.queues[name]; ok {
		return queue
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := make(chan Event)
	f.queues[name] = c
	return c
}

func (f *MemoryQueueFactory) NewSourceChan(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error) {
	result := make(chan Event)
	for _, topic := range config.Topics {
		t := topic
		go func() {
			c := f.getOrCreateChan(t)
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-c:
					result <- event
				}
			}
		}()
	}
	return result, nil
}

func (f *MemoryQueueFactory) NewSinkChan(ctx context.Context, config *SinkQueueConfig) (chan<- Event, error) {
	return f.getOrCreateChan(config.Topic), nil
}
