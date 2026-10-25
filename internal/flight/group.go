package flight

import (
	"context"
	"sync"
)

type call[T any] struct {
	done chan struct{}
	val  T
}

// Group coalesces concurrent calls that share a key into a single execution.
type Group[T any] struct {
	mu sync.Mutex
	m  map[string]*call[T]
}

// start returns the call for key and whether the caller started it, and so has
// to run it.
func (g *Group[T]) start(key string) (c *call[T], leader bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.m == nil {
		g.m = make(map[string]*call[T])
	}
	if c, ok := g.m[key]; ok {
		return c, false
	}
	c = &call[T]{done: make(chan struct{})}
	g.m[key] = c
	return c, true
}

// run executes fn for the call that start returned to its leader.
func (g *Group[T]) run(key string, c *call[T], fn func() T) T {
	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		close(c.done)
	}()
	c.val = fn()
	return c.val
}

// Do runs fn once for all concurrent callers with the same key and gives each
// of them its result. If fn panics, the key is released and waiting callers
// get the zero value instead of blocking forever; the panic continues in the
// caller that ran fn.
func (g *Group[T]) Do(key string, fn func() T) T {
	c, leader := g.start(key)
	if !leader {
		<-c.done
		return c.val
	}
	return g.run(key, c, fn)
}

// DoContext is Do for callers that can give up. A caller that joins a call
// already under way stops waiting, with ctx's error, when ctx is done; the call
// itself carries on for the others. The caller that starts the call runs fn
// itself and returns when it finishes.
func (g *Group[T]) DoContext(ctx context.Context, key string, fn func() T) (T, error) {
	c, leader := g.start(key)
	if leader {
		return g.run(key, c, fn), nil
	}
	select {
	case <-c.done:
		return c.val, nil
	case <-ctx.Done():
		var zero T
		return zero, ctx.Err()
	}
}
