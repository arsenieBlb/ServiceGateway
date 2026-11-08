package gateway

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"service-gateway/internal/config"
)

func reviewServer(t *testing.T, g *Gateway) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(g.Handler())
	s.Config.ReadHeaderTimeout = time.Duration(g.c.Timeouts.ReadHeader)
	s.Config.ReadTimeout = time.Duration(g.c.Timeouts.ReadBody)
	s.Config.IdleTimeout = time.Duration(g.c.Timeouts.Idle)
	s.Config.MaxHeaderBytes = g.c.Limits.MaxHeaderBytes
	s.Start()
	t.Cleanup(s.Close)
	return s
}

func reviewDial(t *testing.T, s *httptest.Server) net.Conn {
	t.Helper()
	c, err := net.DialTimeout("tcp", s.Listener.Addr().String(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	if err := c.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	return c
}

func TestReviewInboundBodyTimeouts(t *testing.T) {
	for _, overall := range []bool{false, true} {
		t.Run(fmt.Sprint(overall), func(t *testing.T) {
			var calls atomic.Int64
			g, _ := limitGateway(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) }, func(c *config.Config) {
				c.Timeouts.Request = config.Duration(2 * time.Second)
				c.Timeouts.ReadBody = config.Duration(50 * time.Millisecond)
				if overall {
					c.Timeouts.Request = config.Duration(50 * time.Millisecond)
					c.Timeouts.ReadBody = config.Duration(time.Second)
				}
			})
			s := reviewServer(t, g)
			conn := reviewDial(t, s)
			_, err := io.WriteString(conn, "POST /api HTTP/1.1\r\nHost: gateway\r\nContent-Length: 10\r\n\r\nx")
			if err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("incomplete body did not return bounded error response: %v", err)
			}
			defer resp.Body.Close()
			want := 408
			if overall {
				want = 504
			}
			if resp.StatusCode != want {
				t.Fatalf("got %d want %d", resp.StatusCode, want)
			}
			if _, err := io.ReadAll(resp.Body); err != nil {
				t.Fatal(err)
			}
			limitEventually(t, func() bool { return g.admit.Snapshot().Active == 0 })
			if calls.Load() != 0 {
				t.Fatal("incomplete body forwarded")
			}
		})
	}
}

func TestReviewInboundHeaderLimits(t *testing.T) {
	var calls atomic.Int64
	g, _ := limitGateway(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) }, func(c *config.Config) {
		c.Limits.MaxHeaderBytes = 1024
		c.Timeouts.ReadHeader = config.Duration(50 * time.Millisecond)
	})
	s := reviewServer(t, g)
	t.Run("oversized", func(t *testing.T) {
		conn := reviewDial(t, s)
		_, err := io.WriteString(conn, "GET /api HTTP/1.1\r\nHost: gateway\r\nX-Large: "+strings.Repeat("x", 20<<10)+"\r\n\r\n")
		if err != nil {
			t.Fatal(err)
		}
		resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		if resp.StatusCode != 431 {
			t.Fatalf("oversized header=%d", resp.StatusCode)
		}
	})
	t.Run("stalled", func(t *testing.T) {
		conn := reviewDial(t, s)
		if _, err := io.WriteString(conn, "GET /api HTTP/1.1\r\nHost: "); err != nil {
			t.Fatal(err)
		}
		_, err := http.ReadResponse(bufio.NewReader(conn), nil)
		if err == nil {
			t.Fatal("incomplete headers accepted")
		}
		var networkError net.Error
		if errors.As(err, &networkError) && networkError.Timeout() {
			t.Fatal("client deadline fired before server closed stalled headers")
		}
	})
	if calls.Load() != 0 {
		t.Fatal("invalid headers forwarded")
	}
}

func TestReviewShutdownInterruptsIncompleteBody(t *testing.T) {
	var calls atomic.Int64
	g, _ := limitGateway(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) }, func(c *config.Config) {
		c.Timeouts.Request = config.Duration(10 * time.Second)
		c.Timeouts.ReadBody = config.Duration(10 * time.Second)
	})
	s := reviewServer(t, g)
	conn := reviewDial(t, s)
	if _, err := io.WriteString(conn, "POST /api HTTP/1.1\r\nHost: gateway\r\nContent-Length: 10\r\n\r\nx"); err != nil {
		t.Fatal(err)
	}
	limitEventually(t, func() bool { return g.admit.Snapshot().Active == 1 })
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- g.Shutdown(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("shutdown: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("shutdown blocked on incomplete inbound body")
	}
	if g.admit.Snapshot().Active != 0 || calls.Load() != 0 {
		t.Fatal("body cancellation leaked or forwarded")
	}
}

func TestReviewUnknownReadinessAndLiveness(t *testing.T) {
	g, _ := limitGateway(t, func(http.ResponseWriter, *http.Request) {}, nil)
	// Construct a fresh unknown pool through a new gateway rather than alter its state.
	unknown, err := New(g.c, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { unknown.Shutdown(context.Background()) })
	for path, want := range map[string]int{"/readyz": 503, "/healthz": 200} {
		w := httptest.NewRecorder()
		unknown.AdminHandler().ServeHTTP(w, httptest.NewRequest("GET", path, nil))
		if w.Code != want {
			t.Fatalf("%s=%d want %d", path, w.Code, want)
		}
	}
	if w := limitRequest(unknown, context.Background(), "GET", "/api", "127.0.0.1:1", nil); w.Code != 503 {
		t.Fatal("unknown backend selected")
	}
}

func TestReviewMalformedUpstreamResponse(t *testing.T) {
	g, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err != nil {
			return
		}
		defer conn.Close()
		io.WriteString(conn, "this is not an HTTP response\r\n\r\n")
	}, nil)
	resp, err := front.Client().Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 502 {
		t.Fatalf("malformed upstream=%d", resp.StatusCode)
	}
	proxyRead(t, resp)
	limitEventually(t, func() bool { _, failures, _ := g.stats.Snapshot(); return failures["connect"] == 1 })
}

func TestReviewEmptyPOSTIsNotReplayed(t *testing.T) {
	var posts atomic.Int64
	_, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Method == "GET" {
			io.WriteString(w, "prime")
			return
		}
		io.Copy(io.Discard, r.Body)
		posts.Add(1)
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	}, nil)
	resp, err := front.Client().Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	proxyRead(t, resp)
	req, _ := http.NewRequest("POST", front.URL+"/api", nil)
	req.Header.Set("Idempotency-Key", "arbitrary-client-value")
	resp, err = front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 502 {
		t.Fatal(resp.StatusCode)
	}
	proxyRead(t, resp)
	if posts.Load() != 1 {
		t.Fatalf("empty POST executed %d times", posts.Load())
	}
}

func TestReviewCapacityRejectsIncompleteBodyPromptly(t *testing.T) {
	started, finish := make(chan struct{}), make(chan struct{})
	g, _ := limitGateway(t, func(w http.ResponseWriter, r *http.Request) {
		close(started)
		select {
		case <-finish:
		case <-r.Context().Done():
		}
	}, func(c *config.Config) { c.Limits.Waiting = 0; c.Timeouts.ReadBody = config.Duration(2 * time.Second) })
	s := reviewServer(t, g)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err := http.Get(s.URL + "/api")
		if err == nil {
			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}
	}()
	proxyWait(t, started)
	defer func() { close(finish); proxyWait(t, done) }()
	conn := reviewDial(t, s)
	conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := io.WriteString(conn, "POST /api HTTP/1.1\r\nHost: gateway\r\nContent-Length: 10\r\n\r\nx"); err != nil {
		t.Fatal(err)
	}
	resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
	if err != nil {
		t.Fatalf("capacity rejection waited for unread request body: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 503 {
		t.Fatalf("capacity rejection=%d", resp.StatusCode)
	}
}

func TestReviewEarlyErrorsDoNotDrainIncompleteBodies(t *testing.T) {
	for _, tc := range []struct {
		name, path, extra, body string
		want                    int
	}{
		{"unmatched", "/unknown", "", "x", 404},
		{"upgrade", "/api", "Upgrade: websocket\r\n", "x", 501},
		{"unhealthy", "/api", "", "x", 503},
		{"rate", "/api", "", "x", 429},
		{"oversized", "/api", "", "123456789", 413},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			g, _ := limitGateway(t, func(http.ResponseWriter, *http.Request) { calls.Add(1) }, func(c *config.Config) {
				c.Limits.BodyBytes = 8
				c.Timeouts.ReadBody = config.Duration(2 * time.Second)
				if tc.name == "rate" {
					c.Rate.Burst = 1
					c.Rate.Refill = .001
				}
			})
			if tc.name == "unhealthy" {
				g.pool.All()[0].Observe(false, 1, 1)
			}
			if tc.name == "rate" {
				g.rate.Allow("127.0.0.1")
			}
			s := reviewServer(t, g)
			conn := reviewDial(t, s)
			conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
			if _, err := io.WriteString(conn, "POST "+tc.path+" HTTP/1.1\r\nHost: gateway\r\nContent-Length: 10\r\n"+tc.extra+"\r\n"+tc.body); err != nil {
				t.Fatal(err)
			}
			resp, err := http.ReadResponse(bufio.NewReader(conn), nil)
			if err != nil {
				t.Fatalf("early error waited for remaining body: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Fatalf("status=%d want%d", resp.StatusCode, tc.want)
			}
			if !resp.Close {
				t.Fatal("rejected incomplete request must close HTTP1 connection")
			}
			if calls.Load() != 0 {
				t.Fatal("rejected work reached upstream")
			}
		})
	}
}
