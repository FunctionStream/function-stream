package memory

import (
	"context"
	"github.com/functionstream/function-stream/fs/api"
	"github.com/functionstream/function-stream/fs/model"
	"go.uber.org/zap"
)

type MemES struct {
	eventLoop chan op
	readCh    map[string]chan api.Event

	log *zap.Logger
}

type op struct {
	registerRead *struct {
		topic string
		c     chan api.Event
	}
	unregisterRead *struct {
		topic string
	}
	write *struct {
		topic string
		event api.Event
	}
}

func (m *MemES) Read(ctx context.Context, topics []model.TopicConfig) (<-chan api.Event, error) {
	m.log.Debug("Reading events", zap.Any("topics", topics))
	c := make(chan api.Event, 10)
	for _, t := range topics {
		m.eventLoop <- op{
			registerRead: &struct {
				topic string
				c     chan api.Event
			}{topic: t.Name, c: c},
		}
	}
	go func() {
		<-ctx.Done()
		for _, t := range topics {
			m.eventLoop <- op{
				unregisterRead: &struct {
					topic string
				}{topic: t.Name},
			}
		}
	}()
	return c, nil
}

func (m *MemES) Write(ctx context.Context, event api.Event, topic model.TopicConfig) error {
	m.log.Debug("Writing event", zap.Any("eventId", event.ID()), zap.Any("topic", topic.Name))
	select {
	case <-ctx.Done():
		return ctx.Err()
	case m.eventLoop <- op{
		write: &struct {
			topic string
			event api.Event
		}{topic: topic.Name, event: event},
	}:
	}
	return nil
}

func (m *MemES) Commit(_ context.Context, eventId string) error {
	m.log.Debug("Committing event", zap.Any("eventId", eventId))
	return nil
}

func NewMemEs(ctx context.Context, log *zap.Logger) api.EventStorage {
	m := &MemES{
		eventLoop: make(chan op, 1000),
		readCh:    make(map[string]chan api.Event),
		log:       log,
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				for _, c := range m.readCh {
					close(c)
				}
				return
			case op := <-m.eventLoop:
				if op.registerRead != nil {
					m.readCh[op.registerRead.topic] = op.registerRead.c
				}
				if op.unregisterRead != nil {
					c := m.readCh[op.unregisterRead.topic]
					delete(m.readCh, op.unregisterRead.topic)
					close(c)
				}
				if op.write != nil {
					c := m.readCh[op.write.topic]
					if c != nil {
						c <- op.write.event
					}
					_ = op.write.event.Commit(ctx)
				}
			}
		}
	}()
	return m
}
