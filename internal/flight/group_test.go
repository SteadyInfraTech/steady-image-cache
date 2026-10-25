package flight

import (
	"context"
	"errors"
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

// A caller that joined a call under way must be able to leave when its own
// request is cancelled, without disturbing the call or the callers still waiting.
func TestDoContextWaiterLeavesWhenCancelled(t *testing.T) {
	var g Group[int]
	release := make(chan struct{})
	started := make(chan struct{})
	leaderDone := make(chan int, 1)
	go func() {
		v, err := g.DoContext(context.Background(), "x", func() int {
			close(started)
			<-release
			return 7
		})
		if err != nil {
			t.Error(err)
		}
		leaderDone <- v
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	waiter := make(chan error, 1)
	go func() {
		_, err := g.DoContext(ctx, "x", func() int { t.Error("second call ran fn"); return 0 })
		waiter <- err
	}()
	cancel()
	select {
	case err := <-waiter:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiter error = %v, want context.Canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("cancelled waiter is still blocked")
	}

	// A patient waiter and the leader still get the result.
	patient := make(chan int, 1)
	go func() {
		v, _ := g.DoContext(context.Background(), "x", func() int { t.Error("patient ran fn"); return 0 })
		patient <- v
	}()
	time.Sleep(20 * time.Millisecond)
	close(release)
	if v := <-leaderDone; v != 7 {
		t.Fatalf("leader got %d", v)
	}
	if v := <-patient; v != 7 {
		t.Fatalf("patient waiter got %d", v)
	}
}
