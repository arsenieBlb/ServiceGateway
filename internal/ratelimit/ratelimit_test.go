package ratelimit

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func (c *fakeClock) Now() time.Time          { c.mu.Lock(); defer c.mu.Unlock(); return c.now }
func (c *fakeClock) advance(d time.Duration) { c.mu.Lock(); defer c.mu.Unlock(); c.now = c.now.Add(d) }
func newClock() *fakeClock                   { return &fakeClock{now: time.Unix(1000, 0)} }

func TestRefillBurstAndClientIsolation(t *testing.T) {
	c := newClock()
	l := New(2, 2, 10, time.Minute, c)
	for i := 0; i < 2; i++ {
		if ok, _, _ := l.Allow("a"); !ok {
			t.Fatal("burst rejected")
		}
	}
	if ok, retry, full := l.Allow("a"); ok || retry != 500*time.Millisecond || full {
		t.Fatalf("limited: %v %v %v", ok, retry, full)
	}
	if ok, _, _ := l.Allow("b"); !ok {
		t.Fatal("client a consumed client b quota")
	}
	c.advance(250 * time.Millisecond)
	if ok, retry, _ := l.Allow("a"); ok || retry != 250*time.Millisecond {
		t.Fatalf("partial refill: %v %v", ok, retry)
	}
	c.advance(250 * time.Millisecond)
	if ok, _, _ := l.Allow("a"); !ok {
		t.Fatal("one token should be refilled")
	}
	c.advance(30 * time.Second)
	for i := 0; i < 2; i++ {
		if ok, _, _ := l.Allow("a"); !ok {
			t.Fatal("full burst not restored")
		}
	}
	if ok, _, _ := l.Allow("a"); ok {
		t.Fatal("refill exceeded burst")
	}
}

func TestIdleExpiryAndFullTablePolicy(t *testing.T) {
	c := newClock()
	l := New(1, 1, 2, 10*time.Second, c)
	l.Allow("a")
	c.advance(2 * time.Second)
	l.Allow("b")
	if ok, retry, full := l.Allow("c"); ok || !full || retry != 8*time.Second {
		t.Fatalf("full table: %v %v %v", ok, retry, full)
	}
	if got := l.Snapshot(); got.Entries != 2 || got.TableFull != 1 || got.Rejected != 1 {
		t.Fatalf("snapshot: %+v", got)
	}
	c.advance(8 * time.Second)
	if ok, _, full := l.Allow("c"); !ok || full {
		t.Fatal("expired entry was not reclaimed")
	}
	if l.Snapshot().Entries != 2 {
		t.Fatal("map exceeded bound")
	}
	c.advance(10 * time.Second)
	if ok, _, _ := l.Allow("d"); !ok {
		t.Fatal("expiry failed")
	}
	if l.Snapshot().Entries != 1 {
		t.Fatal("expired entries retained")
	}
}

func TestRejectedExistingClientRemainsActive(t *testing.T) {
	c := newClock()
	l := New(0.01, 1, 1, 10*time.Second, c)
	l.Allow("a")
	c.advance(9 * time.Second)
	if ok, _, _ := l.Allow("a"); ok {
		t.Fatal("unexpected refill")
	}
	c.advance(2 * time.Second)
	if ok, retry, full := l.Allow("b"); ok || !full || retry != 8*time.Second {
		t.Fatalf("existing rejection must update idle time: %v %v %v", ok, retry, full)
	}
}

func TestConcurrentRequestsNeverExceedBurstOrTable(t *testing.T) {
	c := newClock()
	l := New(1, 7, 3, time.Minute, c)
	var accepted atomic.Int64
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if ok, _, _ := l.Allow("same"); ok {
				accepted.Add(1)
			}
		}()
	}
	wg.Wait()
	if accepted.Load() != 7 {
		t.Fatalf("accepted %d, expected burst 7", accepted.Load())
	}
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(i int) { defer wg.Done(); l.Allow(string(rune(i + 100))) }(i)
	}
	wg.Wait()
	if got := l.Snapshot(); got.Entries != 3 || got.TableFull != 198 || got.Rejected != 391 {
		t.Fatalf("snapshot: %+v", got)
	}
}
