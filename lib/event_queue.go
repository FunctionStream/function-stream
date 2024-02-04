package lib

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
)

type Event func() ([]byte, func())

type EventQueueFactory func(ctx context.Context, function *model.Function) (EventQueue, error)

type EventQueue interface {
	GetSendChan() chan<- Event
	GetRecvChan() <-chan Event
	Close()
}
