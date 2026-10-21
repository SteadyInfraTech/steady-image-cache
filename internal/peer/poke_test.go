package peer

import (
	"context"
	"sync"
	"testing"
	"time"
)

// listings runs a Kubernetes finder's loop with a fake listing that records
// when it was called.
type listings struct {
	mu    sync.Mutex
	times []time.Time
	k     *Kubernetes
	stop  context.CancelFunc
	done  chan struct{}
}

func runLoop(t *testing.T, interval, minGap time.Duration) *listings {
	t.Helper()
	l := &listings{k: &Kubernetes{}, done: make(chan struct{})}
	l.k.setup()
	l.k.minGap = minGap
	l.k.list = func(context.Context) error {
		l.mu.Lock()
		l.times = append(l.times, time.Now())
		l.mu.Unlock()
		return nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	l.stop = cancel
	go func() { l.k.Run(ctx, interval, func(string, ...any) {}); close(l.done) }()
	t.Cleanup(func() { cancel(); <-l.done })
	return l
}

func (l *listings) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.times)
}

func (l *listings) waitFor(t *testing.T, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for l.count() < n {
		if time.Now().After(deadline) {
			t.Fatalf("%d listings after %s, want %d", l.count(), within, n)
		}
		time.Sleep(2 * time.Millisecond)
	}
}

func TestPokeListsBeforeTheIntervalIsUp(t *testing.T) {
	l := runLoop(t, time.Hour, 10*time.Millisecond)
	l.waitFor(t, 1, 2*time.Second)
	l.k.Poke()
	l.waitFor(t, 2, 2*time.Second)
}

func TestNoPokeNoExtraListing(t *testing.T) {
	l := runLoop(t, time.Hour, 10*time.Millisecond)
	l.waitFor(t, 1, 2*time.Second)
	time.Sleep(100 * time.Millisecond)
	if n := l.count(); n != 1 {
		t.Fatalf("%d listings with no poke and a one hour interval, want 1", n)
	}
}

func TestListingsAreNeverCloserThanMinGap(t *testing.T) {
	const gap = 150 * time.Millisecond
	l := runLoop(t, time.Hour, gap)
	l.waitFor(t, 1, 2*time.Second)
	// Poke steadily for long enough to span several gaps.
	end := time.Now().Add(5 * gap)
	for time.Now().Before(end) {
		l.k.Poke()
		time.Sleep(2 * time.Millisecond)
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if len(l.times) < 3 {
		t.Fatalf("only %d listings while being poked for %s", len(l.times), 5*gap)
	}
	for i := 1; i < len(l.times); i++ {
		// A little slack for timer granularity.
		if d := l.times[i].Sub(l.times[i-1]); d < gap-10*time.Millisecond {
			t.Fatalf("listings %d and %d are %s apart, want at least %s", i-1, i, d, gap)
		}
	}
	// And not far fewer than the limit allows: five gaps is about five listings.
	if len(l.times) > 8 {
		t.Fatalf("%d listings in %s with a gap of %s", len(l.times), 5*gap, gap)
	}
}

// Pokes made while a listing is being waited for are that listing, not more.
func TestPokesDuringTheWaitAreOneListing(t *testing.T) {
	const gap = 150 * time.Millisecond
	l := runLoop(t, time.Hour, gap)
	l.waitFor(t, 1, 2*time.Second)
	for i := 0; i < 50; i++ {
		l.k.Poke()
	}
	l.waitFor(t, 2, 2*time.Second)
	// Had one poke stayed queued, a third listing would follow one gap later.
	time.Sleep(2 * gap)
	if n := l.count(); n != 2 {
		t.Fatalf("%d listings, want 2: pokes were not coalesced", n)
	}
}

func TestPokeNeverBlocks(t *testing.T) {
	k := &Kubernetes{}
	k.setup()
	done := make(chan struct{})
	go func() {
		for i := 0; i < 10000; i++ {
			k.Poke() // nobody is running the loop
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Poke blocked")
	}
}

func TestRunStopsWhileWaitingOutTheGap(t *testing.T) {
	l := runLoop(t, time.Hour, time.Hour)
	l.waitFor(t, 1, 2*time.Second)
	l.k.Poke() // now waiting for an hour-long gap
	time.Sleep(20 * time.Millisecond)
	l.stop()
	select {
	case <-l.done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop")
	}
}

type countingFinder struct{ pokes int }

func (c *countingFinder) Candidates() []string { return nil }
func (c *countingFinder) Poke()                { c.pokes++ }

func TestMultiPokesThoseThatCanBePoked(t *testing.T) {
	a, b := &countingFinder{}, &countingFinder{}
	m := Multi{NewStatic([]string{"http://x"}), a, b}
	m.Poke()
	m.Poke()
	if a.pokes != 2 || b.pokes != 2 {
		t.Fatalf("pokes %d and %d, want 2 and 2", a.pokes, b.pokes)
	}
	var _ Poker = m
	var _ Poker = (*Kubernetes)(nil)
}
