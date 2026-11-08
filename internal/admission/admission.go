// Package admission bounds active upstream work and the number of request
// handlers allowed to wait for it. It does not buffer waiting request bodies.
package admission

import (
	"context"
	"errors"
	"sync"
	"time"
)

var (
	ErrFull    = errors.New("admission capacity exhausted")
	ErrTimeout = errors.New("admission wait expired")
	ErrStopped = errors.New("admission stopped")
)

type Snapshot struct {
	Active     int `json:"active"`
	Waiting    int `json:"waiting"`
	MaxActive  int `json:"max_active"`
	MaxWaiting int `json:"max_waiting"`
}

type waiter struct {
	ready   chan struct{}
	granted bool
	err     error
}

type Limiter struct {
	mu                            sync.Mutex
	activeLimit, waitingLimit     int
	wait                          time.Duration
	active, maxActive, maxWaiting int
	queue                         []*waiter
	stopped                       bool
}

func New(active, waiting int, wait time.Duration) *Limiter {
	if active < 1 || waiting < 0 || wait <= 0 {
		panic("admission: invalid limits")
	}
	return &Limiter{activeLimit: active, waitingLimit: waiting, wait: wait}
}

// Acquire waits in the calling handler, without spawning a worker. Every
// successful call returns an idempotent release function, owned by the caller.
func (l *Limiter) Acquire(ctx context.Context) (func(), error) {
	l.mu.Lock()
	if err := ctx.Err(); err != nil {
		l.mu.Unlock()
		return nil, err
	}
	if l.stopped {
		l.mu.Unlock()
		return nil, ErrStopped
	}
	if l.active < l.activeLimit && len(l.queue) == 0 {
		l.addActive()
		l.mu.Unlock()
		return l.releaseFunc(), nil
	}
	if len(l.queue) >= l.waitingLimit {
		l.mu.Unlock()
		return nil, ErrFull
	}
	w := &waiter{ready: make(chan struct{})}
	l.queue = append(l.queue, w)
	if len(l.queue) > l.maxWaiting {
		l.maxWaiting = len(l.queue)
	}
	l.mu.Unlock()

	timer := time.NewTimer(l.wait)
	defer timer.Stop()
	var waitErr error
	select {
	case <-w.ready:
	case <-ctx.Done():
		waitErr = ctx.Err()
	case <-timer.C:
		waitErr = ErrTimeout
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	// Cancellation wins even if a permit became available at the same instant.
	if err := ctx.Err(); err != nil {
		waitErr = err
	}
	if w.err != nil {
		return nil, w.err
	}
	if waitErr != nil {
		if w.granted {
			l.active--
			l.promote()
		} else {
			l.remove(w)
		}
		return nil, waitErr
	}
	return l.releaseFunc(), nil
}

func (l *Limiter) addActive() {
	l.active++
	if l.active > l.maxActive {
		l.maxActive = l.active
	}
}

func (l *Limiter) releaseFunc() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			l.mu.Lock()
			defer l.mu.Unlock()
			l.active--
			l.promote()
		})
	}
}

func (l *Limiter) promote() {
	if l.stopped || len(l.queue) == 0 {
		return
	}
	w := l.queue[0]
	l.queue[0] = nil
	l.queue = l.queue[1:]
	l.addActive()
	w.granted = true
	close(w.ready)
}

func (l *Limiter) remove(w *waiter) {
	for i, pending := range l.queue {
		if pending == w {
			copy(l.queue[i:], l.queue[i+1:])
			l.queue[len(l.queue)-1] = nil
			l.queue = l.queue[:len(l.queue)-1]
			return
		}
	}
}

// Stop prevents new admission and wakes every queued caller. Already admitted
// requests retain their permits until their owner releases them.
func (l *Limiter) Stop() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.stopped {
		return
	}
	l.stopped = true
	for _, w := range l.queue {
		w.err = ErrStopped
		close(w.ready)
	}
	l.queue = nil
}

func (l *Limiter) Snapshot() Snapshot {
	l.mu.Lock()
	defer l.mu.Unlock()
	return Snapshot{Active: l.active, Waiting: len(l.queue), MaxActive: l.maxActive, MaxWaiting: l.maxWaiting}
}
