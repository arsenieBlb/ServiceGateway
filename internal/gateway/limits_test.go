package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"service-gateway/internal/config"
	"service-gateway/internal/metrics"
)

func limitEventually(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			t.Fatal("condition did not settle")
		}
		runtime.Gosched()
	}
}

func limitGateway(t *testing.T, handler http.HandlerFunc, change func(*config.Config)) (*Gateway, *httptest.Server) {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	c := config.Default()
	c.Routes = []config.Route{{ID: "api", Prefix: "/api", Backends: []string{"local"}}}
	c.Upstreams = []config.Upstream{{ID: "local", URL: backend.URL, HealthPath: "/health"}}
	c.Limits.Active, c.Limits.Waiting, c.Limits.Wait = 1, 1, config.Duration(2*time.Second)
	c.Rate.Refill, c.Rate.Burst = 100000, 100000
	if change != nil {
		change(&c)
	}
	g, err := New(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.pool.All()[0].Observe(true, 1, 1)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = g.Shutdown(ctx)
	})
	return g, backend
}

func limitRequest(g *Gateway, ctx context.Context, method, path, peer string, body io.Reader) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, "http://gateway"+path, body).WithContext(ctx)
	r.RemoteAddr = peer
	w := httptest.NewRecorder()
	g.Handler().ServeHTTP(w, r)
	return w
}

type limitReader struct{ reads atomic.Int64 }

func (r *limitReader) Read(p []byte) (int, error) { r.reads.Add(1); return 0, io.EOF }

func TestLimitsCapacityQueuedBodyAndCancellation(t *testing.T) {
	started, upstreamCancelled := make(chan struct{}), make(chan struct{})
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-r.Context().Done()
		close(upstreamCancelled)
	}, nil)
	activeCtx, cancelActive := context.WithCancel(context.Background())
	defer cancelActive()
	activeDone := make(chan struct{})
	go func() { defer close(activeDone); limitRequest(g, activeCtx, "GET", "/api", "127.0.0.1:1", nil) }()
	<-started
	queuedCtx, cancelQueued := context.WithCancel(context.Background())
	defer cancelQueued()
	queuedBody := &limitReader{}
	queuedDone := make(chan struct{})
	go func() { defer close(queuedDone); limitRequest(g, queuedCtx, "POST", "/api", "127.0.0.2:2", queuedBody) }()
	limitEventually(t, func() bool { return g.admit.Snapshot().Waiting == 1 })
	if queuedBody.reads.Load() != 0 {
		t.Fatal("waiting body was read")
	}
	excessBody := &limitReader{}
	if w := limitRequest(g, context.Background(), "POST", "/api", "127.0.0.3:3", excessBody); w.Code != 503 {
		t.Fatalf("capacity status %d: %s", w.Code, w.Body.String())
	}
	if excessBody.reads.Load() != 0 {
		t.Fatal("rejected body was read")
	}
	cancelQueued()
	<-queuedDone
	if got := g.admit.Snapshot(); got.Waiting != 0 || got.Active != 1 {
		t.Fatalf("queued cancel: %+v", got)
	}
	cancelActive()
	<-activeDone
	select {
	case <-upstreamCancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream context not cancelled")
	}
	if got := g.admit.Snapshot(); got.Active != 0 || got.Waiting != 0 || got.MaxActive != 1 || got.MaxWaiting != 1 {
		t.Fatalf("settled: %+v", got)
	}
}

func TestLimitsWaitExpiryAndNoForwarding(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	var calls atomic.Int64
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		close(started)
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	}, func(c *config.Config) { c.Limits.Wait = config.Duration(20 * time.Millisecond) })
	done := make(chan struct{})
	go func() { defer close(done); limitRequest(g, context.Background(), "GET", "/api", "127.0.0.1:1", nil) }()
	<-started
	w := limitRequest(g, context.Background(), "GET", "/api", "127.0.0.2:2", nil)
	if w.Code != 503 || !strings.Contains(w.Body.String(), "admission_rejected") {
		t.Fatalf("wait expiry: %d %s", w.Code, w.Body.String())
	}
	if g.admit.Snapshot().Waiting != 0 || calls.Load() != 1 {
		t.Fatal("expired work retained or forwarded")
	}
	close(finish)
	<-done
}

func TestLimitsBodyAndPolicyPrecedence(t *testing.T) {
	var calls atomic.Int64
	var received string
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		b, _ := io.ReadAll(r.Body)
		received = string(b)
		w.WriteHeader(201)
	}, func(c *config.Config) {
		c.Limits.BodyBytes = 8
		c.Rate.Refill, c.Rate.Burst = 0.001, 1
	})
	ctx := context.Background()
	w := limitRequest(g, ctx, "POST", "/api", "127.0.0.1:1", strings.NewReader("123456789"))
	if w.Code != 413 || calls.Load() != 0 {
		t.Fatalf("oversize forwarded/status: %d %d", calls.Load(), w.Code)
	}
	w = limitRequest(g, ctx, "POST", "/api", "127.0.0.1:2", strings.NewReader("123456789"))
	if w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatalf("rate should precede body: %d", w.Code)
	}
	if w = limitRequest(g, ctx, "GET", "/missing", "127.0.0.1:3", nil); w.Code != 404 {
		t.Fatalf("routing should precede rate: %d", w.Code)
	}
	if w = limitRequest(g, ctx, "CONNECT", "/missing", "127.0.0.1:4", nil); w.Code != 501 {
		t.Fatalf("protocol should precede routing: %d", w.Code)
	}
	if w = limitRequest(g, ctx, "POST", "/api", "127.0.0.2:1", strings.NewReader("12345678")); w.Code != 201 || received != "12345678" {
		t.Fatalf("bounded body reconstruction: %d %q", w.Code, received)
	}
	g.pool.All()[0].Observe(false, 1, 1)
	w = limitRequest(g, ctx, "POST", "/api", "127.0.0.3:1", strings.NewReader("123456789"))
	if w.Code != 503 || !strings.Contains(w.Body.String(), "no_healthy_upstream") {
		t.Fatalf("health should precede body: %d %s", w.Code, w.Body.String())
	}
	if calls.Load() != 1 {
		t.Fatalf("unexpected forwarding: %d", calls.Load())
	}
	if w = limitRequest(g, ctx, "GET", "/api", "127.0.0.3:2", nil); w.Code != 429 {
		t.Fatalf("rate should precede health: %d", w.Code)
	}
}

func TestLimitsGracefulShutdownDrainsActiveAndStopsQueue(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/health" {
			w.WriteHeader(200)
			return
		}
		close(started)
		select {
		case <-finish:
		case <-r.Context().Done():
		}
		w.WriteHeader(201)
	}, nil)
	g.StartHealth(context.Background())
	activeResult, queuedResult := make(chan *httptest.ResponseRecorder, 1), make(chan *httptest.ResponseRecorder, 1)
	go func() { activeResult <- limitRequest(g, context.Background(), "GET", "/api", "127.0.0.1:1", nil) }()
	<-started
	go func() { queuedResult <- limitRequest(g, context.Background(), "GET", "/api", "127.0.0.2:2", nil) }()
	limitEventually(t, func() bool { return g.admit.Snapshot().Waiting == 1 })
	shutdownResult := make(chan error, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	go func() { shutdownResult <- g.Shutdown(ctx) }()
	select {
	case w := <-queuedResult:
		if w.Code != 503 {
			t.Fatalf("shutdown queued status %d", w.Code)
		}
	case <-ctx.Done():
		t.Fatal("shutdown did not wake queued request")
	}
	select {
	case err := <-shutdownResult:
		t.Fatalf("shutdown did not drain active: %v", err)
	default:
	}
	if w := limitRequest(g, context.Background(), "GET", "/api", "127.0.0.3:3", nil); w.Code != 503 {
		t.Fatalf("new work admitted during shutdown: %d", w.Code)
	}
	close(finish)
	if w := <-activeResult; w.Code != 201 {
		t.Fatalf("admitted request not drained: %d", w.Code)
	}
	if err := <-shutdownResult; err != nil {
		t.Fatal(err)
	}
	if got := g.admit.Snapshot(); got.Active != 0 || got.Waiting != 0 {
		t.Fatalf("shutdown leaked permits: %+v", got)
	}
	for path, want := range map[string]int{"/readyz": 503, "/healthz": 200} {
		w := httptest.NewRecorder()
		g.AdminHandler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s=%d expected%d", path, w.Code, want)
		}
	}
}

func TestLimitsShutdownDeadlineCancelsActiveUpstream(t *testing.T) {
	started, cancelled := make(chan struct{}), make(chan struct{})
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(cancelled) }, nil)
	done := make(chan struct{})
	go func() { defer close(done); limitRequest(g, context.Background(), "GET", "/api", "127.0.0.1:1", nil) }()
	<-started
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if err := g.Shutdown(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("shutdown deadline: %v", err)
	}
	select {
	case <-cancelled:
	case <-time.After(time.Second):
		t.Fatal("upstream work survived shutdown")
	}
	<-done
	if g.admit.Snapshot().Active != 0 {
		t.Fatal("shutdown leaked admission")
	}
}

func TestLimitsMetricsReconcileAndDimensionsStayBounded(t *testing.T) {
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("error") != "" {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(204)
	}, nil)
	ctx := context.Background()
	for i := 0; i < 10; i++ {
		if w := limitRequest(g, ctx, "GET", "/api/unique/"+strings.Repeat("x", i)+"?secret=value", "127.0.0.1:1", nil); w.Code != 204 {
			t.Fatal(w.Code)
		}
	}
	limitRequest(g, ctx, "GET", "/api?error=yes", "127.0.0.1:1", nil)
	for i := 0; i < 7; i++ {
		limitRequest(g, ctx, "GET", "/missing/"+strings.Repeat("z", i), "127.0.0.1:1", nil)
	}
	w := httptest.NewRecorder()
	g.AdminHandler().ServeHTTP(w, httptest.NewRequest("GET", "/metrics", nil))
	var result struct {
		Requests map[string]metrics.Route `json:"requests"`
		Failures map[string]uint64        `json:"transport_failures"`
		Backends map[string]uint64        `json:"backend_requests"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if len(result.Requests) != 2 || len(result.Backends) != 1 || len(result.Failures) != 4 {
		t.Fatalf("unbounded dimensions: %s", w.Body.String())
	}
	api, missing := result.Requests["api"], result.Requests[""]
	if api.Total != 11 || missing.Total != 7 || api.StatusClasses["2xx"] != 10 || api.StatusClasses["5xx"] != 1 || missing.StatusClasses["4xx"] != 7 || result.Backends["local"] != 11 {
		t.Fatalf("unreconciled metrics: %s", w.Body.String())
	}
	if api.LatencyBuckets["+Inf"] != 11 || missing.LatencyBuckets["+Inf"] != 7 {
		t.Fatal("latency samples did not reconcile")
	}
	for _, failures := range result.Failures {
		if failures != 0 {
			t.Fatal("backend 500 counted as transport failure")
		}
	}
	if strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "unique") || strings.Contains(w.Body.String(), "request_id") {
		t.Fatal("untrusted labels leaked into metrics")
	}
}
