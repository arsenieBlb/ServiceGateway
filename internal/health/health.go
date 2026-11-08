// Package health runs serial, bounded probes for each configured backend.
package health

import (
	"context"
	"io"
	"net"
	"net/http"
	"service-gateway/internal/config"
	"service-gateway/internal/upstreams"
	"sync"
	"time"
)

type Checker struct {
	pool   *upstreams.Pool
	config config.Health
	client *http.Client
	mu     sync.Mutex
	active map[*upstreams.Backend]bool
}

func New(pool *upstreams.Pool, c config.Health) *Checker {
	transport := &http.Transport{
		DialContext:  (&net.Dialer{Timeout: time.Duration(c.Timeout)}).DialContext,
		MaxIdleConns: len(pool.All()), MaxIdleConnsPerHost: 1, MaxConnsPerHost: 1,
		IdleConnTimeout: 30 * time.Second, TLSHandshakeTimeout: time.Duration(c.Timeout), ResponseHeaderTimeout: time.Duration(c.Timeout),
	}
	return &Checker{pool: pool, config: c, client: &http.Client{Transport: transport, Timeout: time.Duration(c.Timeout), CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}, active: make(map[*upstreams.Backend]bool)}
}

// Check performs a single probe. Concurrent checks for the same backend are skipped.
// Only 2xx is healthy; redirects are never followed to a different target.
func (c *Checker) Check(ctx context.Context, b *upstreams.Backend) bool {
	c.mu.Lock()
	if c.active[b] {
		c.mu.Unlock()
		return false
	}
	c.active[b] = true
	c.mu.Unlock()
	defer func() { c.mu.Lock(); delete(c.active, b); c.mu.Unlock() }()
	probeCtx, cancel := context.WithTimeout(ctx, time.Duration(c.config.Timeout))
	defer cancel()
	req, err := http.NewRequestWithContext(probeCtx, http.MethodGet, b.HealthURL.String(), nil)
	success := false
	if err == nil {
		resp, requestErr := c.client.Do(req)
		if requestErr == nil {
			success = resp.StatusCode >= 200 && resp.StatusCode < 300
			_, drainErr := io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
			success = success && drainErr == nil
			_ = resp.Body.Close()
		}
	}
	if ctx.Err() == nil {
		b.Observe(success, c.config.SuccessThreshold, c.config.FailureThreshold)
	}
	return success
}

// Run joins all gateway-owned workers before returning after cancellation.
func (c *Checker) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, b := range c.pool.All() {
		wg.Add(1)
		go func(b *upstreams.Backend) {
			defer wg.Done()
			ticker := time.NewTicker(time.Duration(c.config.Interval))
			defer ticker.Stop()
			for {
				if ctx.Err() != nil {
					return
				}
				c.Check(ctx, b)
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
				}
			}
		}(b)
	}
	wg.Wait()
	c.client.CloseIdleConnections()
}
