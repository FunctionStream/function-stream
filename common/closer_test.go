package common

import (
	"errors"
	"github.com/hashicorp/go-multierror"
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
				return NewCloser(context.Background()), nil
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
				return ac, multierror.Append(nil, expectedErr)
			},
		},
		{
			name: "Multiple Closers",
			setup: func() (Closer, error) {
				c := NewCloser(context.Background())
				_ = c.AddChildCloser(NewCloser(context.Background()))
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
				c := NewCloser(context.Background())
				_ = c.AddChildCloser(NewCloser(context.Background()))
				child2 := c.AddChildCloser(NewAsyncCloser(context.Background())).(AsyncCloser)
				err := errors.New("expected error")
				go func() {
					<-child2.OnClose()
					child2.FinishClosing(err)
				}()
				return c, multierror.Append(err, nil)
			},
		},
		{
			name: "Multiple Closers with parent closer errors",
			setup: func() (Closer, error) {
				c := NewCloser(context.Background())
				child1 := c.AddChildCloser(NewAsyncCloser(context.Background())).(AsyncCloser)
				child2 := child1.AddChildCloser(NewAsyncCloser(context.Background())).(AsyncCloser)
				err := errors.New("expected error")
				go func() {
					<-child1.OnClose()
					child1.FinishClosing(err)
				}()
				go func() {
					<-child2.OnClose()
					child2.FinishClosing(nil)
				}()
				return c, multierror.Append(err, nil)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			closer, expectedErr := tc.setup()
			assert.Nil(t, closer.Err())
			err := closer.Close()
			slog.Info("Closed", slog.Any("err", err))
			assert.Equal(t, expectedErr, err)
			assert.Equal(t, expectedErr, closer.Err())
		})
	}
}

func TestCloserCloseMultipleTimes(t *testing.T) {
	closer := NewCloser(context.Background())
	assert.Nil(t, closer.Close())
	assert.Equal(t, ErrAlreadyClosed, closer.Close())
}
