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
