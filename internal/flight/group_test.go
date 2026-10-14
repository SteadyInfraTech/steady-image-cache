package flight

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestCoalesces(t *testing.T) {
	var g Group
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
	var g Group
	var n int
	for i := 0; i < 3; i++ {
		g.Do("x", func() bool { n++; return true })
	}
	if n != 3 {
		t.Fatalf("sequential calls should each run, got %d", n)
	}
}

func TestPanicReleasesWaiters(t *testing.T) {
	var g Group
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
