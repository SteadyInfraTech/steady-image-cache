package flight

import "sync"

type call struct {
	done chan struct{}
	ok   bool
}

// Group coalesces concurrent calls that share a key into a single execution.
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

// Do runs fn once for all concurrent callers with the same key and gives each
// of them its result. If fn panics, the key is released and waiting callers
// get false instead of blocking forever; the panic continues in the caller
// that ran fn.
func (g *Group) Do(key string, fn func() bool) bool {
	g.mu.Lock()
	if g.m == nil {
		g.m = make(map[string]*call)
	}
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		<-c.done
		return c.ok
	}
	c := &call{done: make(chan struct{})}
	g.m[key] = c
	g.mu.Unlock()
	defer func() {
		g.mu.Lock()
		delete(g.m, key)
		g.mu.Unlock()
		close(c.done)
	}()
	c.ok = fn()
	return c.ok
}
