package lifecycle

import (
	"github.com/functionstream/function-stream/common"
	"github.com/pkg/errors"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestLifecycle_AddCloseHook(t *testing.T) {
	l := NewLifecycle()
	c, f := l.AddCloseHook("test")
	err := errors.New("expected error")
	go func() {
		<-c
		f(err)
	}()
	assert.ErrorIs(t, l.Close(), err)
	assert.ErrorIs(t, l.CheckState(), err)
}

func TestLifecycle_CheckState(t *testing.T) {
	l := NewLifecycle()
	assert.NoError(t, l.CheckState())
	assert.NoError(t, l.Close())
	assert.ErrorIs(t, l.CheckState(), common.ErrAlreadyClosed)
}
