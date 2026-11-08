// Package gateway wires policy around a standard HTTP reverse proxy.
package gateway

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"math"
	"net"
	"net/http"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"service-gateway/internal/admission"
	"service-gateway/internal/config"
	"service-gateway/internal/health"
	"service-gateway/internal/metrics"
	proxyhttp "service-gateway/internal/proxy"
	"service-gateway/internal/ratelimit"
	"service-gateway/internal/routing"
	"service-gateway/internal/upstreams"
)

type Gateway struct {
	c            config.Config
	pool         *upstreams.Pool
	router       *routing.Router
	admit        *admission.Limiter
	rate         *ratelimit.Limiter
	stats        *metrics.Registry
	transport    *http.Transport
	log          *slog.Logger
	ctx          context.Context
	cancel       context.CancelFunc
	healthCancel context.CancelFunc
	healthOnce   sync.Once
	healthWG     sync.WaitGroup
	mu           sync.Mutex
	closing      bool
	workers      sync.WaitGroup
	stopped      atomic.Bool
}

func New(c config.Config, logger *slog.Logger) (*Gateway, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if logger == nil {
		logger = slog.New(slog.NewJSONHandler(io.Discard, nil))
	}
	ctx, cancel := context.WithCancel(context.Background())
	g := &Gateway{c: c, pool: upstreams.New(c.Upstreams), router: routing.New(c.Routes), admit: admission.New(c.Limits.Active, c.Limits.Waiting, time.Duration(c.Limits.Wait)), rate: ratelimit.New(c.Rate.Refill, c.Rate.Burst, c.Rate.MaxEntries, time.Duration(c.Rate.IdleTTL), nil), log: logger, ctx: ctx, cancel: cancel}
	rids := []string{}
	bids := []string{}
	for _, r := range c.Routes {
		rids = append(rids, r.ID)
	}
	for _, b := range c.Upstreams {
		bids = append(bids, b.ID)
	}
	g.stats = metrics.New(rids, bids)
	g.transport = &http.Transport{Proxy: nil, DialContext: (&net.Dialer{Timeout: time.Duration(c.Timeouts.Connect), KeepAlive: 30 * time.Second}).DialContext, ForceAttemptHTTP2: true, MaxIdleConns: c.Timeouts.MaxConnsPerHost * len(c.Upstreams), MaxIdleConnsPerHost: c.Timeouts.MaxConnsPerHost, MaxConnsPerHost: c.Timeouts.MaxConnsPerHost, IdleConnTimeout: time.Duration(c.Timeouts.Idle), TLSHandshakeTimeout: time.Duration(c.Timeouts.Connect), ResponseHeaderTimeout: time.Duration(c.Timeouts.ResponseHeader), ExpectContinueTimeout: time.Second, MaxResponseHeaderBytes: int64(c.Limits.MaxHeaderBytes)}
	return g, nil
}

// StartHealth starts at most one checker and returns immediately. Shutdown joins it.
func (g *Gateway) StartHealth(parent context.Context) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.closing {
		return
	}
	g.healthOnce.Do(func() {
		ctx, cancel := context.WithCancel(parent)
		g.healthCancel = cancel
		g.healthWG.Add(1)
		go func() { defer g.healthWG.Done(); health.New(g.pool, g.c.Health).Run(ctx) }()
	})
}
func (g *Gateway) Handler() http.Handler { return http.HandlerFunc(g.serve) }
func (g *Gateway) AdminHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) { writeJSON(w, 200, map[string]bool{"alive": true}) })
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		ready := !g.stopped.Load() && g.pool.Ready(g.c.Routes)
		status := 200
		if !ready {
			status = 503
		}
		writeJSON(w, status, map[string]bool{"ready": ready})
	})
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		routes, failures, backends := g.stats.Snapshot()
		states := []map[string]string{}
		for _, b := range g.pool.All() {
			states = append(states, map[string]string{"id": b.ID, "state": b.State()})
		}
		writeJSON(w, 200, map[string]any{"requests": routes, "transport_failures": failures, "backend_requests": backends, "admission": g.admit.Snapshot(), "rate_limit": g.rate.Snapshot(), "upstreams": states})
	})
	return mux
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *responseWriter) WriteHeader(status int) {
	if status >= 100 && status < 200 {
		w.ResponseWriter.WriteHeader(status)
		return
	}
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return w.ResponseWriter.Write(p)
}
func (w *responseWriter) FlushError() error {
	if w.status == 0 {
		w.WriteHeader(200)
	}
	return http.NewResponseController(w.ResponseWriter).Flush()
}

func (g *Gateway) serve(w http.ResponseWriter, r *http.Request) {
	clientCtx := r.Context()
	start := time.Now()
	id := requestID()
	w.Header().Set("X-Request-ID", id)
	rw := &responseWriter{ResponseWriter: w}
	routeID, backendID, outcome := "", "", "ok"
	tracked := false
	closeInput := false
	defer func() {
		p := recover()
		if p != nil {
			outcome = "partial"
		}
		g.stats.Record(routeID, rw.status, outcome, backendID, time.Since(start))
		g.log.Info("request", "request_id", id, "route", routeID, "backend", backendID, "status", rw.status, "outcome", outcome, "duration_ms", time.Since(start).Milliseconds())
		if tracked {
			g.workers.Done()
		}
		if p != nil {
			panic(p)
		}
	}()
	fail := func(status int, code string) {
		outcome = code
		if clientCtx.Err() == nil || code == "timeout" || code == "body_timeout" {
			// Rejected bodies must not be drained by net/http before the error
			// becomes visible, nor leave a server worker waiting on a slow sender.
			closeInput = true
			if r.ProtoMajor == 1 {
				rw.Header().Set("Connection", "close")
			}
			_ = http.NewResponseController(rw).SetReadDeadline(time.Now())
			writeJSON(rw, status, map[string]string{"error": code, "request_id": id})
		}
	}
	g.mu.Lock()
	if g.closing {
		g.mu.Unlock()
		fail(503, "stopping")
		return
	}
	g.workers.Add(1)
	tracked = true
	g.mu.Unlock()
	ctx, cancel := context.WithTimeout(r.Context(), time.Duration(g.c.Timeouts.Request))
	defer cancel()
	stop := context.AfterFunc(g.ctx, cancel)
	defer stop()
	r = r.WithContext(ctx)
	if r.Method == http.MethodConnect || r.Header.Get("Upgrade") != "" {
		fail(501, "unsupported_protocol")
		return
	}
	route, ok := g.router.Match(r.URL.Path)
	if !ok {
		fail(404, "not_found")
		return
	}
	routeID = route.ID
	peer, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		peer = r.RemoteAddr
	}
	allowed, retry, full := g.rate.Allow(peer)
	if !allowed {
		rw.Header().Set("Retry-After", strconv.Itoa(max(1, int(math.Ceil(retry.Seconds())))))
		if full {
			fail(429, "limiter_full")
		} else {
			fail(429, "rate_limited")
		}
		return
	}
	if !g.hasHealthy(route.Backends) {
		fail(503, "no_healthy_upstream")
		return
	}
	release, err := g.admit.Acquire(ctx)
	if err != nil {
		if clientCtx.Err() != nil {
			outcome = "canceled"
			return
		}
		fail(503, "admission_rejected")
		return
	}
	defer release()
	backend := g.pool.Pick(route.Backends)
	if backend == nil {
		fail(503, "no_healthy_upstream")
		return
	}
	backendID = backend.ID
	controller := http.NewResponseController(rw)
	deadline, _ := ctx.Deadline()
	// A short bounded write allowance lets an overall deadline produce its 504.
	_ = controller.SetWriteDeadline(deadline.Add(100 * time.Millisecond))
	callbackDone := make(chan struct{})
	var readMu sync.Mutex
	reading := true
	interrupt := context.AfterFunc(ctx, func() {
		defer close(callbackDone)
		readMu.Lock()
		if reading {
			_ = controller.SetReadDeadline(time.Now())
		}
		readMu.Unlock()
		if errors.Is(ctx.Err(), context.Canceled) {
			_ = controller.SetWriteDeadline(time.Now())
		}
	})
	defer func() {
		if !interrupt() {
			<-callbackDone
		}
		if closeInput {
			_ = controller.SetReadDeadline(time.Now())
		} else {
			_ = controller.SetReadDeadline(time.Time{})
		}
		_ = controller.SetWriteDeadline(time.Time{})
	}()
	readDeadline := time.Now().Add(time.Duration(g.c.Timeouts.ReadBody))
	if deadline.Before(readDeadline) {
		readDeadline = deadline
	}
	_ = controller.SetReadDeadline(readDeadline)
	// Only admitted bodies are read; never size allocations from Content-Length.
	body, err := io.ReadAll(io.LimitReader(r.Body, g.c.Limits.BodyBytes+1))
	readMu.Lock()
	reading = false
	_ = controller.SetReadDeadline(time.Time{})
	readMu.Unlock()
	if err != nil {
		var ne net.Error
		if errors.Is(ctx.Err(), context.DeadlineExceeded) || !time.Now().Before(deadline) {
			fail(504, "timeout")
			return
		}
		if errors.As(err, &ne) && ne.Timeout() && g.ctx.Err() == nil {
			fail(408, "body_timeout")
			return
		}
		if clientCtx.Err() != nil || g.ctx.Err() != nil {
			outcome = "canceled"
			return
		}
		fail(400, "body_read")
		return
	}
	if int64(len(body)) > g.c.Limits.BodyBytes {
		fail(413, "body_too_large")
		return
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	r.GetBody = nil
	upstreamError := func(w http.ResponseWriter, req *http.Request, err error) {
		if r.Context().Err() != nil && errors.Is(r.Context().Err(), context.Canceled) {
			outcome = "canceled"
			return
		}
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			fail(504, "timeout")
		} else {
			fail(502, "connect")
		}
	}
	p := proxyhttp.New(backend.URL, g.transport, id, upstreamError, func(err error) { outcome = "partial" })
	p.ServeHTTP(rw, r)
}
func (g *Gateway) hasHealthy(ids []string) bool {
	for _, b := range g.pool.All() {
		for _, id := range ids {
			if b.ID == id && b.Healthy() {
				return true
			}
		}
	}
	return false
}

// Shutdown stops the queue, joins probes, drains handlers, then cancels any work
// remaining at the caller's deadline. The HTTP server must also be shut down.
func (g *Gateway) Shutdown(ctx context.Context) error {
	g.mu.Lock()
	g.closing = true
	g.stopped.Store(true)
	if g.healthCancel != nil {
		g.healthCancel()
	}
	g.mu.Unlock()
	g.admit.Stop()
	done := make(chan struct{})
	go func() { g.workers.Wait(); close(done) }()
	var err error
	select {
	case <-done:
	case <-ctx.Done():
		err = ctx.Err()
		g.cancel()
		<-done
	}
	g.cancel()
	g.healthWG.Wait()
	g.transport.CloseIdleConnections()
	return err
}
func requestID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("random source unavailable")
	}
	return hex.EncodeToString(b[:])
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
