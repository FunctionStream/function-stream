package lib

import (
	"context"
	"github.com/functionstream/functionstream/common/model"
)

type EventQueueFactory interface {
	NewEventQueue(ctx context.Context, function *model.Function) (EventQueue, error)
}
