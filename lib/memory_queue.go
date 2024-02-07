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
	"sync/atomic"
)

type queue struct {
	c      chan Event
	refCnt int32
}

type MemoryQueueFactory struct {
	mu     sync.Mutex
	queues map[string]*queue
}

func NewMemoryQueueFactory() EventQueueFactory {
	return &MemoryQueueFactory{
		queues: make(map[string]*queue),
	}
}

func (f *MemoryQueueFactory) getOrCreateChan(name string) chan Event {
	if q, ok := f.queues[name]; ok {
		atomic.AddInt32(&q.refCnt, 1)
		return q.c
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	c := make(chan Event)
	f.queues[name] = &queue{
		c:      c,
		refCnt: 1,
	}
	return c
}

func (f *MemoryQueueFactory) release(name string) {
	q, ok := f.queues[name]
	if !ok {
		panic("release non-exist queue: " + name)
	}
	if atomic.AddInt32(&q.refCnt, -1) == 0 {
		f.mu.Lock()
		close(q.c)
		delete(f.queues, name)
		f.mu.Unlock()
	}
}

func (f *MemoryQueueFactory) NewSourceChan(ctx context.Context, config *SourceQueueConfig) (<-chan Event, error) {
	result := make(chan Event)
	for _, topic := range config.Topics {
		t := topic
		go func() {
			<-ctx.Done()
			f.release(t)
		}()
		go func() {
			c := f.getOrCreateChan(t)
			defer close(result)
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
	c := f.getOrCreateChan(config.Topic)
	go func() {
		<-ctx.Done()
		f.release(config.Topic)
	}()
	return c, nil
}
