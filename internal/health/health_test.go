package health

import (
	"context"
	"net/http"
	"net/http/httptest"
	"service-gateway/internal/config"
	"service-gateway/internal/upstreams"
	"sync/atomic"
	"testing"
	"time"
)

func setup(t *testing.T, h http.HandlerFunc) (*Checker, *upstreams.Backend) {
	t.Helper()
	s := httptest.NewServer(h)
	t.Cleanup(s.Close)
	p := upstreams.New([]config.Upstream{{ID: "test", URL: s.URL, HealthPath: "/healthz"}})
	c := New(p, config.Health{Interval: config.Duration(time.Hour), Timeout: config.Duration(100 * time.Millisecond), SuccessThreshold: 2, FailureThreshold: 2})
	t.Cleanup(c.client.CloseIdleConnections)
	return c, p.All()[0]
}

func TestProbeTransitionsAndRecovery(t *testing.T) {
	var status atomic.Int32
	status.Store(200)
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Error(r.URL.Path)
		}
		w.WriteHeader(int(status.Load()))
	})
	ctx := context.Background()
	c.Check(ctx, b)
	if b.Healthy() {
		t.Fatal("early healthy")
	}
	c.Check(ctx, b)
	if !b.Healthy() {
		t.Fatal("not healthy")
	}
	status.Store(503)
	c.Check(ctx, b)
	if !b.Healthy() {
		t.Fatal("flapped")
	}
	c.Check(ctx, b)
	if b.Healthy() {
		t.Fatal("not unhealthy")
	}
	status.Store(200)
	c.Check(ctx, b)
	c.Check(ctx, b)
	if !b.Healthy() {
		t.Fatal("not recovered")
	}
}

func TestProbeDoesNotFollowRedirect(t *testing.T) {
	var visits atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { visits.Add(1) }))
	defer target.Close()
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) { http.Redirect(w, r, target.URL, http.StatusFound) })
	if c.Check(context.Background(), b) || visits.Load() != 0 {
		t.Fatal("followed health redirect")
	}
}

func TestNoOverlappingProbeAndCancellation(t *testing.T) {
	entered, exited := make(chan struct{}), make(chan struct{})
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) { close(entered); <-r.Context().Done(); close(exited) })
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("probe did not start")
	}
	if c.Check(ctx, b) {
		t.Fatal("overlapping check not skipped")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("health workers leaked")
	}
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("upstream context not cancelled")
	}
	if b.State() != "unknown" {
		t.Fatal("shutdown counted as failed probe")
	}
	c.mu.Lock()
	active := len(c.active)
	c.mu.Unlock()
	if active != 0 {
		t.Fatalf("active probes=%d", active)
	}
}

func TestProbeDeadline(t *testing.T) {
	finished := make(chan struct{})
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done(); close(finished) })
	c.config.FailureThreshold = 1
	start := time.Now()
	if c.Check(context.Background(), b) {
		t.Fatal("hanging probe healthy")
	}
	if time.Since(start) > time.Second {
		t.Fatal("probe exceeded bound")
	}
	if b.State() != "unhealthy" {
		t.Fatal("timeout not observed")
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("probe context leaked")
	}
}

func TestProbeBodyDeadlineCountsFailure(t *testing.T) {
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	c.config.FailureThreshold = 1
	if c.Check(context.Background(), b) || b.State() != "unhealthy" {
		t.Fatal("a hanging health body must fail within the probe deadline")
	}
}

func TestRunContinuesProbingAfterUnhealthy(t *testing.T) {
	var status atomic.Int32
	status.Store(503)
	c, b := setup(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(int(status.Load())) })
	c.config.Interval = config.Duration(time.Millisecond)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { c.Run(ctx); close(done) }()
	defer func() { cancel(); <-done }()
	waitState := func(want string) {
		t.Helper()
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
		tick := time.NewTicker(time.Millisecond)
		defer tick.Stop()
		for b.State() != want {
			select {
			case <-deadline.C:
				t.Fatalf("state %q, wanted %q", b.State(), want)
			case <-tick.C:
			}
		}
	}
	waitState("unhealthy")
	status.Store(200)
	waitState("healthy")
}
