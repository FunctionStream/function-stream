package lib

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
)

type Event func() ([]byte, func())

type QueueConfig struct {
	Inputs []string
	Output string
}

type EventQueueFactory func(ctx context.Context, config *QueueConfig, function *model.Function) (EventQueue, error)

type EventQueue interface {
	GetSendChan() (chan<- Event, error)
	GetRecvChan() (<-chan Event, error)
}
