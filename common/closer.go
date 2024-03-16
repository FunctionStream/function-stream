package common

import (
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"golang.org/x/net/context"
	"io"
	"sync"
	"sync/atomic"
)

var (
	ErrAlreadyClosed = errors.New("already closed")
)

type Closer interface {
	io.Closer
	OnClose() <-chan struct{}
	AddChildCloser(Closer) Closer
	Err() error
}

type AsyncCloser interface {
	Closer
	FinishClosing(error)
}

type closer struct {
	io.Closer
	ctx        context.Context
	cancelFunc context.CancelFunc

	err      atomic.Value
	mu       sync.Mutex
	children map[Closer]struct{}
}

func newCloser(ctx context.Context) *closer {
	ctx, cancel := context.WithCancel(ctx)
	return &closer{
		ctx:        ctx,
		cancelFunc: cancel,
	}
}

func NewCloser(ctx context.Context) Closer {
	return newCloser(ctx)
}

func NewAsyncCloser(ctx context.Context) AsyncCloser {
	return &asyncCloser{
		closer: newCloser(ctx),
		errCh:  make(chan error),
	}
}

func (c *closer) AddChildCloser(child Closer) Closer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.children == nil {
		c.children = make(map[Closer]struct{})
	}
	c.children[child] = struct{}{}
	return child
}

func (c *closer) close() error {
	if err := c.Err(); err != nil {
		return ErrAlreadyClosed
	}
	c.cancelFunc()

	c.mu.Lock()
	defer c.mu.Unlock()
	var err error
	for child := range c.children {
		e := child.Close()
		if e != nil {
			err = multierror.Append(err, e)
		}
		delete(c.children, child)
	}
	return err
}

func (c *closer) Close() error {
	if err := c.Err(); err != nil {
		return ErrAlreadyClosed
	}

	err := c.close()

	if err != nil {
		c.err.Store(err)
	} else {
		c.err.Store(ErrAlreadyClosed)
	}

	return err
}

func (c *closer) OnClose() <-chan struct{} {
	return c.ctx.Done()
}

func (c *closer) Err() error {
	e := c.err.Load()
	if e == nil {
		return nil
	}
	return e.(error)
}

type asyncCloser struct {
	*closer
	errCh chan error
}

func (c *asyncCloser) Close() error {
	err := c.closer.close()
	if errors.Is(err, ErrAlreadyClosed) {
		return ErrAlreadyClosed
	}

	asyncErr := <-c.errCh
	if err != nil || asyncErr != nil {
		err = multierror.Append(err, asyncErr)
		c.err.Store(err)
		return err
	}
	c.err.Store(ErrAlreadyClosed)
	return nil
}

func (c *asyncCloser) FinishClosing(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errCh <- err
	close(c.errCh)
}
