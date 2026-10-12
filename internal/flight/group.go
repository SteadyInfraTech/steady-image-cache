package flight

import "sync"

type call struct {
	done chan struct{}
	ok   bool
}
type Group struct {
	mu sync.Mutex
	m  map[string]*call
}

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
	c.ok = fn()
	close(c.done)
	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.ok
}
