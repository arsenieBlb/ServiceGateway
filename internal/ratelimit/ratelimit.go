// Package ratelimit implements a bounded, per-process client token bucket.
package ratelimit

import (
	"math"
	"sync"
	"time"
)

type Clock interface{ Now() time.Time }
type realClock struct{}

func (realClock) Now() time.Time { return time.Now() }

type bucket struct {
	tokens        float64
	updated, seen time.Time
}

type Snapshot struct {
	Entries   int    `json:"entries"`
	Rejected  uint64 `json:"rejected"`
	TableFull uint64 `json:"table_full"`
}

type Limiter struct {
	mu                  sync.Mutex
	clock               Clock
	refill              float64
	burst, maxEntries   int
	idleTTL             time.Duration
	entries             map[string]*bucket
	rejected, tableFull uint64
}

func New(refill float64, burst, maxEntries int, idleTTL time.Duration, clock Clock) *Limiter {
	if refill <= 0 || math.IsNaN(refill) || math.IsInf(refill, 0) || burst < 1 || maxEntries < 1 || idleTTL <= 0 {
		panic("ratelimit: invalid limits")
	}
	if clock == nil {
		clock = realClock{}
	}
	return &Limiter{clock: clock, refill: refill, burst: burst, maxEntries: maxEntries, idleTTL: idleTTL, entries: make(map[string]*bucket)}
}

// Allow rejects new identities when the table is full rather than evicting an
// active client's bucket and resetting its quota. Idle entries expire lazily on
// the next request. A rejection counts as activity for an existing identity.
// retryAfter is the earliest estimated retry time, never a reservation.
func (l *Limiter) Allow(client string) (allowed bool, retryAfter time.Duration, tableFull bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := l.clock.Now()
	soonestExpiry := l.idleTTL
	for key, entry := range l.entries {
		remaining := l.idleTTL - now.Sub(entry.seen)
		if remaining <= 0 {
			delete(l.entries, key)
		} else if remaining < soonestExpiry {
			soonestExpiry = remaining
		}
	}
	b := l.entries[client]
	if b == nil {
		if len(l.entries) >= l.maxEntries {
			l.rejected++
			l.tableFull++
			return false, soonestExpiry, true
		}
		b = &bucket{tokens: float64(l.burst), updated: now, seen: now}
		l.entries[client] = b
	}
	if elapsed := now.Sub(b.updated); elapsed > 0 {
		b.tokens = math.Min(float64(l.burst), b.tokens+elapsed.Seconds()*l.refill)
		b.updated = now
	}
	b.seen = now
	if b.tokens >= 1 {
		b.tokens--
		return true, 0, false
	}
	l.rejected++
	nanoseconds := math.Ceil((1 - b.tokens) / l.refill * float64(time.Second))
	if nanoseconds >= float64(math.MaxInt64) {
		return false, time.Duration(math.MaxInt64), false
	}
	retryAfter = time.Duration(nanoseconds)
	if retryAfter < 1 {
		retryAfter = 1
	}
	return false, retryAfter, false
}

func (l *Limiter) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Snapshot{Entries: len(l.entries), Rejected: l.rejected, TableFull: l.tableFull}
}
