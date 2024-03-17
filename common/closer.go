package common

import (
	"github.com/hashicorp/go-multierror"
	"github.com/pkg/errors"
	"github.com/sasha-s/go-deadlock"
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
	// OnClose Deprecate
	// Deprecated
	OnClose() <-chan struct{} // TODO: Move to AsyncCloser
	AddCloseHook(name string) (<-chan struct{}, func(err error))
	AddChildCloser(Closer) Closer
	Err() error
}

// AsyncCloser is a Closer that can be closed asynchronously.
// Deprecated
type AsyncCloser interface {
	Closer
	// FinishClosing
	// Deprecated
	FinishClosing(error)
}

type hook struct {
	name string
	ch   chan error
}

type closer struct {
	io.Closer
	//ctx        context.Context
	doneCh     <-chan struct{}
	cancelFunc context.CancelFunc
	closeFunc  func() error
	closeOnce  sync.Once

	parent Closer

	err      atomic.Value
	mu       deadlock.Mutex
	children map[Closer]struct{}
	hooks    map[*hook]struct{}
}

func newCloser(ctx context.Context) *closer {
	ctx, cancel := context.WithCancel(ctx)
	return &closer{
		doneCh:     ctx.Done(),
		cancelFunc: cancel,
	}
}

func NewDefaultCloser() Closer {
	return newCloser(context.Background())
}

func NewCloser(doneCh <-chan struct{}, cancelFunc context.CancelFunc, closeFunc func() error) Closer {
	return &closer{
		doneCh:     doneCh,
		cancelFunc: cancelFunc,
		closeFunc:  closeFunc,
	}
}

func NewCloserWithFunc(f func() error) Closer {
	c := newCloser(context.Background())
	c.closeFunc = f
	return c
}

func NewAsyncCloser(ctx context.Context) AsyncCloser {
	return &asyncCloser{
		closer: newCloser(ctx),
		errCh:  make(chan error, 1),
	}
}

func (c *closer) AddChildCloser(child Closer) Closer {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.children == nil {
		c.children = make(map[Closer]struct{})
	}
	c.children[child] = struct{}{}
	if childCloser, ok := child.(*closer); ok {
		childCloser.parent = c
	}
	return child
}

func (c *closer) removeChild(child Closer) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.children == nil {
		return false
	}
	_, ok := c.children[child]
	if ok {
		delete(c.children, child)
	}
	return ok
}

func (c *closer) close() error {
	if err := c.Err(); err != nil {
		return err
	}

	if c.closeFunc != nil {
		var err error
		c.closeOnce.Do(func() {
			err = c.closeFunc()
		})
		if err != nil {
			return err
		}
	}

	c.cancelFunc()

	c.mu.Lock()
	var err error
	for h := range c.hooks {
		e := <-h.ch
		if e != nil {
			err = multierror.Append(err, errors.Wrap(e, "failed to close hook: "+h.name))
		} else {
			delete(c.hooks, h)
		}
	}

	// Copy the children to close, to avoid locking while closing them.
	childrenToClose := make([]Closer, 0, len(c.children))
	for child := range c.children {
		childrenToClose = append(childrenToClose, child)
	}
	c.mu.Unlock()

	// Close each child without holding the c.mu lock.
	for _, child := range childrenToClose {
		e := child.Close()
		if e != nil {
			err = multierror.Append(err, e)
		}
	}

	// Now remove the child, reacquiring the lock to do so safely.
	c.mu.Lock()
	for _, child := range childrenToClose {
		delete(c.children, child)
	}
	c.mu.Unlock()
	return err
}

func (c *closer) Close() error {
	if err := c.Err(); err != nil {
		return err
	}

	err := c.close()

	if err != nil {
		c.err.Store(err)
	} else {
		c.err.Store(ErrAlreadyClosed)
	}

	if p, ok := c.parent.(*closer); ok {
		p.removeChild(c)
	}

	return err
}

func (c *closer) OnClose() <-chan struct{} {
	return c.doneCh
}

func (c *closer) AddCloseHook(name string) (<-chan struct{}, func(err error)) {
	c.mu.Lock()
	defer c.mu.Unlock()
	h := &hook{
		name: name,
		ch:   make(chan error),
	}
	if c.hooks == nil {
		c.hooks = make(map[*hook]struct{})
	}
	c.hooks[h] = struct{}{}
	return c.doneCh, func(err error) {
		h.ch <- err
		close(h.ch)
	}
}

func (c *closer) Err() error {
	e := c.err.Load()
	if e == nil {
		return nil
	}
	if !errors.Is(e.(error), ErrAlreadyClosed) {
		return errors.Wrap(e.(error), "closer has been closed but failed")
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
