package lib

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
)

type Event interface {
	GetPayload() []byte
	Ack()
}

type QueueConfig struct {
	Topics []string
}

type EventQueueFactory func(ctx context.Context, config *QueueConfig, function *model.Function) (EventQueue, error)

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
