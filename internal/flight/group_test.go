package flight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalesces(t *testing.T) {
	var g Group[bool]
	var n atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok := g.Do("x", func() bool {
				n.Add(1)
				time.Sleep(20 * time.Millisecond)
				return true
			})
			if !ok {
				t.Error("false")
			}
		}()
	}
	wg.Wait()
	if n.Load() != 1 {
		t.Fatalf("runs=%d", n.Load())
	}
}

func TestKeyIsReleasedAfterCompletion(t *testing.T) {
	var g Group[bool]
	var n int
	for i := 0; i < 3; i++ {
		g.Do("x", func() bool { n++; return true })
	}
	if n != 3 {
		t.Fatalf("sequential calls should each run, got %d", n)
	}
}

func TestPanicReleasesWaiters(t *testing.T) {
	var g Group[bool]
	started := make(chan struct{})
	release := make(chan struct{})
	leader := make(chan any, 1)
	go func() {
		defer func() { leader <- recover() }()
		g.Do("k", func() bool {
			close(started)
			<-release
			panic("boom")
		})
	}()
	<-started

	waiter := make(chan bool, 1)
	go func() { waiter <- g.Do("k", func() bool { return true }) }()
	time.Sleep(20 * time.Millisecond)
	close(release)

	if r := <-leader; r == nil {
		t.Fatal("the leader should have panicked")
	}
	select {
	case <-waiter:
	case <-time.After(2 * time.Second):
		t.Fatal("waiter is stuck after the leader panicked")
	}
	if !g.Do("k", func() bool { return true }) {
		t.Fatal("key was not released after the panic")
	}
}

// Waiters get the leader's whole result, not just a success flag.
func TestWaitersShareTheLeadersResult(t *testing.T) {
	type result struct {
		ok     bool
		status int
	}
	var g Group[result]
	started := make(chan struct{})
	release := make(chan struct{})
	got := make(chan result, 2)
	go func() {
		got <- g.Do("k", func() result {
			close(started)
			<-release
			return result{false, 507}
		})
	}()
	<-started
	go func() { got <- g.Do("k", func() result { return result{true, 0} }) }()
	time.Sleep(20 * time.Millisecond)
	close(release)
	for i := 0; i < 2; i++ {
		if r := <-got; r != (result{false, 507}) {
			t.Fatalf("caller %d got %+v, want the leader's result", i, r)
		}
	}
}
