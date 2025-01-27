package memory

import (
	"context"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

type queue struct {
	c      chan api.Event
	refCnt int32
}

type MemEventStorage struct {
	mu     sync.Mutex
	queues map[string]*queue

	log *zap.Logger
}

func (es *MemEventStorage) release(name string) {
	es.mu.Lock()
	defer es.mu.Unlock()
	q, ok := es.queues[name]
	if !ok {
		panic("release non-exist queue: " + name)
	}
	if atomic.AddInt32(&q.refCnt, -1) == 0 {
		close(q.c)
		delete(es.queues, name)
	}

	es.log.Info("Released memory queue", zap.String("name", name), zap.Int32("current_use_count", atomic.LoadInt32(&q.refCnt)))
}

func (es *MemEventStorage) getOrCreateChan(name string) chan api.Event {
	es.mu.Lock()
	defer es.mu.Unlock()
	if q, ok := es.queues[name]; ok {
		atomic.AddInt32(&q.refCnt, 1)
		return q.c
	}
	es.log.Info("Creating memory queue", zap.String("name", name))
	c := make(chan api.Event, 100)
	es.queues[name] = &queue{
		c:      c,
		refCnt: 1,
	}
	return c
}

func (es *MemEventStorage) Read(ctx context.Context, topics []model.TopicConfig) (<-chan api.Event, error) {
	result := make(chan api.Event)
	if es.log.Core().Enabled(zap.DebugLevel) {
		es.log.Debug("Reading events", zap.Any("topics", topics))
	}

	var wg sync.WaitGroup
	for _, topic := range topics {
		t := topic.Name
		wg.Add(1)
		go func() {
			<-ctx.Done()
			es.release(t)
		}()
		c := es.getOrCreateChan(t)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case event := <-c:
					if es.log.Core().Enabled(zap.DebugLevel) {
						es.log.Debug("Receive event", zap.Any("eventId", event.ID()), zap.Any("topic", t))
					}
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

func (es *MemEventStorage) Write(ctx context.Context, event api.Event, topic model.TopicConfig) error {
	c := es.getOrCreateChan(topic.Name)
	if es.log.Core().Enabled(zap.DebugLevel) {
		es.log.Debug("Writing event", zap.Any("eventId", event.ID()), zap.Any("topic", topic.Name))
	}
	defer es.release(topic.Name)
	select {
	case <-ctx.Done():
		return ctx.Err()
	case c <- event:
		return event.Commit(ctx)
	}
}

func (es *MemEventStorage) Commit(_ context.Context, _ string) error {
	return nil
}

func NewMemoryEventStorage(log *zap.Logger) api.EventStorage {
	return &MemEventStorage{
		queues: make(map[string]*queue),
		log:    log,
	}
}
