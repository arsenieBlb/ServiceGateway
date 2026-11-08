package admission

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"
)

func waitFor(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not become true")
		}
		runtime.Gosched()
	}
}

func TestBoundedActiveAndWaiting(t *testing.T) {
	l := New(4, 8, 3*time.Second)
	var held []func()
	for i := 0; i < 4; i++ {
		release, err := l.Acquire(context.Background())
		if err != nil {
			t.Fatal(err)
		}
		held = append(held, release)
	}
	results := make(chan error, 20)
	for i := 0; i < 20; i++ {
		go func() {
			release, err := l.Acquire(context.Background())
			if err == nil {
				release()
			}
			results <- err
		}()
	}
	waitFor(t, func() bool { return l.Snapshot().Waiting == 8 && len(results) == 12 })
	if got := l.Snapshot(); got.Active != 4 || got.Waiting != 8 {
		t.Fatalf("at capacity: %+v", got)
	}
	for _, release := range held {
		release()
	}
	full, admitted := 0, 0
	for i := 0; i < 20; i++ {
		switch err := <-results; {
		case err == nil:
			admitted++
		case errors.Is(err, ErrFull):
			full++
		default:
			t.Fatal(err)
		}
	}
	if full != 12 || admitted != 8 {
		t.Fatalf("full=%d admitted=%d", full, admitted)
	}
	if got := l.Snapshot(); got.Active != 0 || got.Waiting != 0 || got.MaxActive != 4 || got.MaxWaiting != 8 {
		t.Fatalf("settled: %+v", got)
	}
}

func TestQueueCancellationAndTimeout(t *testing.T) {
	l := New(1, 1, 30*time.Millisecond)
	release, _ := l.Acquire(context.Background())
	defer release()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() { _, err := l.Acquire(ctx); result <- err }()
	waitFor(t, func() bool { return l.Snapshot().Waiting == 1 })
	cancel()
	if err := <-result; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancel: %v", err)
	}
	if l.Snapshot().Waiting != 0 {
		t.Fatal("cancelled waiter retained")
	}
	_, err := l.Acquire(context.Background())
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("timeout: %v", err)
	}
	if l.Snapshot().Waiting != 0 {
		t.Fatal("timed-out waiter retained")
	}
}

func TestStopWakesQueuedAndDrainsActive(t *testing.T) {
	l := New(1, 1, time.Hour)
	release, _ := l.Acquire(context.Background())
	result := make(chan error, 1)
	go func() { _, err := l.Acquire(context.Background()); result <- err }()
	waitFor(t, func() bool { return l.Snapshot().Waiting == 1 })
	l.Stop()
	l.Stop()
	select {
	case err := <-result:
		if !errors.Is(err, ErrStopped) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("Stop did not wake waiter")
	}
	if got := l.Snapshot(); got.Active != 1 || got.Waiting != 0 {
		t.Fatalf("stopped: %+v", got)
	}
	if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrStopped) {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); release() }()
	}
	wg.Wait()
	if l.Snapshot().Active != 0 {
		t.Fatal("release was not idempotent")
	}
}

func TestCancelAndGrantRaceDoesNotLeak(t *testing.T) {
	for i := 0; i < 300; i++ {
		l := New(1, 1, time.Second)
		held, _ := l.Acquire(context.Background())
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			release, err := l.Acquire(ctx)
			if err == nil {
				release()
			} else if !errors.Is(err, context.Canceled) {
				t.Errorf("acquire: %v", err)
			}
		}()
		waitFor(t, func() bool { return l.Snapshot().Waiting == 1 })
		var wg sync.WaitGroup
		wg.Add(2)
		go func() { defer wg.Done(); held() }()
		go func() { defer wg.Done(); cancel() }()
		wg.Wait()
		<-done
		if got := l.Snapshot(); got.Active != 0 || got.Waiting != 0 {
			t.Fatalf("iteration %d leaked: %+v", i, got)
		}
	}
}

func TestZeroQueueAndCancelledImmediate(t *testing.T) {
	l := New(1, 0, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := l.Acquire(ctx); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	release, _ := l.Acquire(context.Background())
	defer release()
	if _, err := l.Acquire(context.Background()); !errors.Is(err, ErrFull) {
		t.Fatal(err)
	}
}
