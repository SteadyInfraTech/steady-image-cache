package flight

import "sync"

type call[T any] struct {
	done chan struct{}
	val  T
}

// Group coalesces concurrent calls that share a key into a single execution.
type Group[T any] struct {
	mu sync.Mutex
	m  map[string]*call[T]
}

// Do runs fn once for all concurrent callers with the same key and gives each
// of them its result. If fn panics, the key is released and waiting callers
// get the zero value instead of blocking forever; the panic continues in the
// caller that ran fn.
func (g *Group[T]) Do(key string, fn func() T) T {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call[T])
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.val
	}
	c := &call[T]{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		close(c.done)
	}()
	c.val = fn()
	return c.val
}
