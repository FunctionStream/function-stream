package common

import (
	"errors"
	"github.com/stretchr/testify/assert"
	"golang.org/x/net/context"
	"log/slog"
	"testing"
)

func TestCloser(t *testing.T) {
	tests := []struct {
		name      string
		setup     func() (Closer, error)
		checkFunc func(error, error) bool
	}{
		{
			name: "Basic closer",
			setup: func() (Closer, error) {
				return NewDefaultCloser(), nil
			},
		},
		{
			name: "Async closer without error",
			setup: func() (Closer, error) {
				ac := NewAsyncCloser(context.Background())
				go func() {
					<-ac.OnClose()
					ac.FinishClosing(nil)
				}()
				return ac, nil
			},
		},
		{
			name: "Async closer with error",
			setup: func() (Closer, error) {
				expectedErr := errors.New("expected error")
				ac := NewAsyncCloser(context.Background())
				go func() {
					<-ac.OnClose()
					ac.FinishClosing(expectedErr)
				}()
				return ac, expectedErr
			},
		},
		{
			name: "Multiple Closers",
			setup: func() (Closer, error) {
				c := NewDefaultCloser()
				_ = c.AddChildCloser(NewDefaultCloser())
				child2 := c.AddChildCloser(NewAsyncCloser(context.Background())).(AsyncCloser)
				go func() {
					<-child2.OnClose()
					child2.FinishClosing(nil)
				}()
				return c, nil
			},
		},
		{
			name: "Multiple Closers with error",
			setup: func() (Closer, error) {
				c := NewDefaultCloser()
				_ = c.AddChildCloser(NewDefaultCloser())
				child2 := c.AddChildCloser(NewAsyncCloser(context.Background())).(AsyncCloser)
				err := errors.New("expected error")
				go func() {
					<-child2.OnClose()
					child2.FinishClosing(err)
				}()
				return c, err
			},
		},
		{
			name: "Multiple Closers with parent closer errors",
			setup: func() (Closer, error) {
				c := NewDefaultCloser()
				child1 := c.AddChildCloser(NewDefaultCloser())
				child2 := c.AddChildCloser(NewDefaultCloser())
				err := errors.New("expected error")
				c1, f1 := child1.AddCloseHook("c1")
				go func() {
					<-c1
					f1(err)
				}()
				c2, f2 := child2.AddCloseHook("c2")
				go func() {
					<-c2
					f2(nil)
				}()
				return c, err
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closer, expectedErr := tc.setup()
			assert.Nil(t, closer.Err())
			err := closer.Close()
			slog.Info("Closed", slog.Any("err", err))
			assert.ErrorIs(t, err, expectedErr)
		})
	}
}

func TestCloserCloseMultipleTimes(t *testing.T) {
	closer := NewDefaultCloser()
	assert.Nil(t, closer.Close())
	assert.Equal(t, ErrAlreadyClosed, closer.Close())
	assert.Equal(t, ErrAlreadyClosed, closer.Err())
}

func TestCloserFunc(t *testing.T) {
	c := NewCloserWithFunc(func() error {
		return nil
	})

	assert.Nil(t, c.Err())
	assert.Nil(t, c.Close())
	assert.ErrorIs(t, c.Close(), ErrAlreadyClosed)
}

func TestCloserFuncError(t *testing.T) {
	expectedErr := errors.New("expected error")
	c := NewCloserWithFunc(func() error {
		return expectedErr
	})

	assert.Nil(t, c.Err())
	assert.Equal(t, expectedErr, c.Close())
	assert.ErrorIs(t, c.Close(), expectedErr)
}
