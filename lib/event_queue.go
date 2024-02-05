package lib

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
)

type Event interface {
	GetPayload() []byte
	Ack()
}

type SourceQueueConfig struct {
	Topics []string
}

type SinkQueueConfig struct {
	Topic string
}

//type EventQueueFactory func(ctx context.Context, config *QueueConfig, function *model.Function) (EventQueue, error)

type EventQueueFactory interface {
	NewSourceChan(ctx context.Context, config *SourceQueueConfig, function *model.Function) (<-chan Event, error)
	NewSinkChan(ctx context.Context, config *SinkQueueConfig, function *model.Function) (chan<- Event, error)
}

type EventQueue interface {
	GetSendChan() (chan<- Event, error)
	GetRecvChan() (<-chan Event, error)
}

type AckableEvent struct {
	payload []byte
	ackFunc func()
}

func NewAckableEvent(payload []byte, ackFunc func()) *AckableEvent {
	return &AckableEvent{
		payload: payload,
		ackFunc: ackFunc,
	}
}

func (e *AckableEvent) GetPayload() []byte {
	return e.payload
}

func (e *AckableEvent) Ack() {
	e.ackFunc()
}
