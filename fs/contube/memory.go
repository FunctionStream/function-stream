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
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

type queue struct {
	c      chan Record
	refCnt int32
}

type MemoryQueueFactory struct {
	ctx    context.Context
	mu     sync.Mutex
	queues map[string]*queue
}

func NewMemoryQueueFactory(ctx context.Context) TubeFactory {
	return &MemoryQueueFactory{
		ctx:    ctx,
		queues: make(map[string]*queue),
	}
}

func (f *MemoryQueueFactory) getOrCreateChan(name string) chan Record {
	f.mu.Lock()
	defer f.mu.Unlock()
	defer func() {
		slog.InfoContext(f.ctx, "Get memory queue chan",
			"current_use_count", atomic.LoadInt32(&f.queues[name].refCnt),
			"name", name)
	}()
	if q, ok := f.queues[name]; ok {
		atomic.AddInt32(&q.refCnt, 1)
		return q.c
	}
	c := make(chan Record, 100)
	f.queues[name] = &queue{
		c:      c,
		refCnt: 1,
	}
	return c
}

func (f *MemoryQueueFactory) release(name string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	q, ok := f.queues[name]
	if !ok {
		panic("release non-exist queue: " + name)
	}
	if atomic.AddInt32(&q.refCnt, -1) == 0 {
		close(q.c)
		delete(f.queues, name)
	}
	slog.InfoContext(f.ctx, "Released memory queue",
		"current_use_count", atomic.LoadInt32(&q.refCnt),
		"name", name)
}

func (f *MemoryQueueFactory) NewSourceTube(ctx context.Context, configMap ConfigMap) (<-chan Record, error) {
	config := SourceQueueConfig{}
	if err := configMap.ToConfigStruct(&config); err != nil {
		return nil, err
	}
	result := make(chan Record)

	var wg sync.WaitGroup
	for _, topic := range config.Topics {
		t := topic
		wg.Add(1)
		go func() {
			<-ctx.Done()
			f.release(t)
		}()
		c := f.getOrCreateChan(t)
		go func() {
			defer wg.Done()
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

	go func() {
		wg.Wait()
		close(result)
	}()

	return result, nil
}

func (f *MemoryQueueFactory) NewSinkTube(ctx context.Context, configMap ConfigMap) (chan<- Record, error) {
	config := SinkQueueConfig{}
	if err := configMap.ToConfigStruct(&config); err != nil {
		return nil, err
	}
	c := f.getOrCreateChan(config.Topic)
	wrapperC := make(chan Record)
	go func() {
		defer f.release(config.Topic)
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-wrapperC:
				if !ok {
					return
				}
				event.Commit()
				c <- event
			}
		}
	}()
	return wrapperC, nil
}
