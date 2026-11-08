package gateway

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"service-gateway/internal/config"
)

func proxyFixture(t *testing.T, handler http.HandlerFunc, change func(*config.Config)) (*Gateway, *httptest.Server, *httptest.Server) {
	t.Helper()
	backend := httptest.NewServer(handler)
	t.Cleanup(backend.Close)
	c := config.Default()
	c.Upstreams = []config.Upstream{{ID: "one", URL: backend.URL, HealthPath: "/health"}}
	c.Routes = []config.Route{{ID: "api", Prefix: "/api", Backends: []string{"one"}}}
	c.Rate.Refill = 1e6
	c.Rate.Burst = 1e6
	if change != nil {
		change(&c)
	}
	g, err := New(c, nil)
	if err != nil {
		t.Fatal(err)
	}
	g.pool.All()[0].Observe(true, 1, 1)
	front := httptest.NewServer(g.Handler())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		g.Shutdown(ctx)
		front.Close()
	})
	return g, front, backend
}
func proxyRead(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()
	b, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
func proxyWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("context/worker did not finish")
	}
}

func TestProxyHTTPContract(t *testing.T) {
	type seen struct{ Method, URI, Host, Body, Forwarded, XFF, XFH, XFP, Evil, Hop, ID string }
	received := make(chan seen, 20)
	_, front, backend := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		received <- seen{r.Method, r.RequestURI, r.Host, string(b), r.Header.Get("Forwarded"), r.Header.Get("X-Forwarded-For"), r.Header.Get("X-Forwarded-Host"), r.Header.Get("X-Forwarded-Proto"), r.Header.Get("X-Forwarded-Evil"), r.Header.Get("X-Hop"), r.Header.Get("X-Request-ID")}
		w.Header().Set("X-Backend", "yes")
		w.Header().Set("Connection", "X-Backend-Hop")
		w.Header().Set("X-Backend-Hop", "drop")
		w.WriteHeader(418)
		if r.Method != "HEAD" {
			io.WriteString(w, "backend response")
		}
	}, nil)
	for _, method := range []string{"GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS"} {
		t.Run(method, func(t *testing.T) {
			req, _ := http.NewRequest(method, front.URL+"/api/a%2Fb?q=x%2By&empty=&q=z", strings.NewReader("payload"))
			req.Host = "original.example"
			req.Header.Set("Forwarded", "for=spoof")
			req.Header.Set("X-Forwarded-For", "192.0.2.7")
			req.Header.Set("X-Forwarded-Host", "spoof")
			req.Header.Set("X-Forwarded-Proto", "https")
			req.Header.Set("X-Forwarded-Evil", "spoof")
			req.Header.Set("Connection", "X-Hop")
			req.Header.Set("X-Hop", "drop")
			req.Header.Set("X-Request-ID", strings.Repeat("bad", 1000))
			resp, err := front.Client().Do(req)
			if err != nil {
				t.Fatal(err)
			}
			body := proxyRead(t, resp)
			got := <-received
			if resp.StatusCode != 418 || resp.Header.Get("X-Backend") != "yes" || resp.Header.Get("X-Backend-Hop") != "" {
				t.Fatalf("response: %v", resp)
			}
			if method != "HEAD" && body != "backend response" {
				t.Fatal(body)
			}
			if got.Method != method || got.URI != "/api/a%2Fb?q=x%2By&empty=&q=z" || got.Body != "payload" || got.Host != strings.TrimPrefix(backend.URL, "http://") {
				t.Fatalf("forwarded %#v", got)
			}
			if got.Forwarded != "" || got.Evil != "" || got.Hop != "" || got.XFF != "127.0.0.1" || got.XFH != "original.example" || got.XFP != "http" {
				t.Fatalf("forwarding %#v", got)
			}
			if len(got.ID) != 32 || resp.Header.Get("X-Request-ID") != got.ID {
				t.Fatalf("request id %#v", got)
			}
		})
	}
}

func TestProxyErrorsAndOrdering(t *testing.T) {
	var hits atomic.Int64
	g, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		w.WriteHeader(500)
		io.WriteString(w, "application error")
	}, func(c *config.Config) { c.Limits.BodyBytes = 8 })
	for _, tc := range []struct {
		method, path, body, upgrade string
		status                      int
		code                        string
	}{{"GET", "/unknown", "", "", 404, "not_found"}, {"CONNECT", "/unknown", "", "", 501, "unsupported_protocol"}, {"GET", "/unknown", "", "websocket", 501, "unsupported_protocol"}, {"POST", "/api", strings.Repeat("a", 9), "", 413, "body_too_large"}} {
		req, _ := http.NewRequest(tc.method, front.URL+tc.path, strings.NewReader(tc.body))
		req.Header.Set("Upgrade", tc.upgrade)
		resp, err := front.Client().Do(req)
		if err != nil {
			t.Fatal(err)
		}
		body := proxyRead(t, resp)
		if resp.StatusCode != tc.status || !strings.Contains(body, tc.code) {
			t.Fatalf("%s: %d %s", tc.path, resp.StatusCode, body)
		}
	}
	if hits.Load() != 0 {
		t.Fatal("rejected request reached backend")
	}
	resp, err := http.Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 500 || proxyRead(t, resp) != "application error" {
		t.Fatal("backend error not preserved")
	}
	_, failures, _ := g.stats.Snapshot()
	for _, n := range failures {
		if n != 0 {
			t.Fatal(failures)
		}
	}
	g.pool.All()[0].Observe(false, 1, 1)
	resp, err = http.Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 503 {
		t.Fatal(resp.StatusCode)
	}
	proxyRead(t, resp)
}

func TestUpstreamRefusalAndDeadlines(t *testing.T) {
	t.Run("refused", func(t *testing.T) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		url := "http://" + l.Addr().String()
		l.Close()
		_, front, _ := proxyFixture(t, func(http.ResponseWriter, *http.Request) {}, func(c *config.Config) { c.Upstreams[0].URL = url })
		resp, err := http.Get(front.URL + "/api")
		if err != nil {
			t.Fatal(err)
		}
		if resp.StatusCode != 502 {
			t.Fatal(resp.StatusCode)
		}
		if !strings.Contains(proxyRead(t, resp), "connect") {
			t.Fatal("missing error")
		}
	})
	for _, overall := range []bool{false, true} {
		t.Run(fmt.Sprintf("overall=%v", overall), func(t *testing.T) {
			ended := make(chan struct{})
			_, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) { <-r.Context().Done(); close(ended) }, func(c *config.Config) {
				if overall {
					c.Timeouts.Request = config.Duration(50 * time.Millisecond)
				} else {
					c.Timeouts.ResponseHeader = config.Duration(50 * time.Millisecond)
				}
			})
			resp, err := http.Get(front.URL + "/api")
			if err != nil {
				t.Fatal(err)
			}
			if resp.StatusCode != 504 {
				t.Fatalf("status %d body %s", resp.StatusCode, proxyRead(t, resp))
			}
			proxyRead(t, resp)
			proxyWait(t, ended)
		})
	}
}

func TestPartialStreamAbortsWithoutJSON(t *testing.T) {
	ended := make(chan struct{})
	g, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(200)
		io.WriteString(w, "first chunk\n")
		w.(http.Flusher).Flush()
		<-r.Context().Done()
		close(ended)
	}, func(c *config.Config) { c.Timeouts.Request = config.Duration(100 * time.Millisecond) })
	resp, err := http.Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	b, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err == nil || resp.StatusCode != 200 || string(b) != "first chunk\n" {
		t.Fatalf("%d %q %v", resp.StatusCode, b, err)
	}
	proxyWait(t, ended)
	// The body termination arrives before the accounting defer necessarily runs.
	deadline := time.Now().Add(time.Second)
	for {
		_, f, _ := g.stats.Snapshot()
		if f["partial"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal(f)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClientDisconnectCancelsUpstream(t *testing.T) {
	started, ended := make(chan struct{}), make(chan struct{})
	g, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) { close(started); <-r.Context().Done(); close(ended) }, nil)
	ctx, cancel := context.WithCancel(context.Background())
	req, _ := http.NewRequestWithContext(ctx, "GET", front.URL+"/api", nil)
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, _ := front.Client().Do(req)
		if resp != nil {
			resp.Body.Close()
		}
	}()
	proxyWait(t, started)
	cancel()
	proxyWait(t, done)
	proxyWait(t, ended)
	deadline := time.Now().Add(time.Second)
	for g.admit.Snapshot().Active != 0 {
		if time.Now().After(deadline) {
			t.Fatal(g.admit.Snapshot())
		}
		time.Sleep(time.Millisecond)
	}
}

func TestPOSTIsNotReplayedAfterAmbiguousFailure(t *testing.T) {
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
	resp, err := http.Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	proxyRead(t, resp)
	req, _ := http.NewRequest("POST", front.URL+"/api", strings.NewReader("side effect"))
	req.Header.Set("Idempotency-Key", "client-controlled")
	resp, err = front.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 502 {
		t.Fatal(resp.StatusCode)
	}
	proxyRead(t, resp)
	if posts.Load() != 1 {
		t.Fatalf("POST executions %d", posts.Load())
	}
}

func TestTLSUpstreamUsesExplicitTrust(t *testing.T) {
	backend := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "secure") }))
	defer backend.Close()
	g, front, _ := proxyFixture(t, func(http.ResponseWriter, *http.Request) {}, func(c *config.Config) { c.Upstreams[0].URL = backend.URL })
	roots := x509.NewCertPool()
	roots.AddCert(backend.Certificate())
	g.transport.TLSClientConfig = &tls.Config{RootCAs: roots, MinVersion: tls.VersionTLS12}
	resp, err := http.Get(front.URL + "/api")
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != 200 || proxyRead(t, resp) != "secure" {
		t.Fatal(resp.StatusCode)
	}
}

func TestAccessLogRedaction(t *testing.T) {
	var logs bytes.Buffer
	g, front, _ := proxyFixture(t, func(w http.ResponseWriter, r *http.Request) { io.WriteString(w, "response-secret") }, nil)
	g.log = slog.New(slog.NewJSONHandler(&logs, nil))
	req, _ := http.NewRequest("POST", front.URL+"/api?token=query-secret", strings.NewReader("body-secret"))
	req.Header.Set("Authorization", "Bearer header-secret")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	proxyRead(t, resp)
	// Wait for handler completion before reading this non-concurrent buffer.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g.Shutdown(ctx)
	for _, s := range []string{"query-secret", "body-secret", "header-secret", "response-secret"} {
		if strings.Contains(logs.String(), s) {
			t.Fatal("secret in logs")
		}
	}
	var row map[string]any
	if err := json.Unmarshal(bytes.TrimSpace(logs.Bytes()), &row); err != nil {
		t.Fatal(err)
	}
	if len(row["request_id"].(string)) != 32 {
		t.Fatal(row)
	}
}
