package api

import (
	"github.com/functionstream/functionstream/common/model"
	"golang.org/x/net/context"
)

type FunctionInstance interface {
	Context() context.Context
	Definition() *model.Function
	Stop()
}
