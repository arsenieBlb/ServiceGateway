// Package upstreams owns health state and concurrency-safe backend selection.
package upstreams

import (
	"net/url"
	"service-gateway/internal/config"
	"strings"
	"sync"
)

type Backend struct {
	ID                  string
	URL                 *url.URL
	HealthURL           *url.URL
	mu                  sync.RWMutex
	state               string
	successes, failures int
}

func (b *Backend) Healthy() bool { return b.State() == "healthy" }
func (b *Backend) State() string { b.mu.RLock(); defer b.mu.RUnlock(); return b.state }

// Observe records a completed probe. A cancelled shutdown probe is not an observation.
func (b *Backend) Observe(success bool, successThreshold, failureThreshold int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if success {
		b.failures = 0
		if b.successes < successThreshold {
			b.successes++
		}
		if b.successes >= successThreshold {
			b.state = "healthy"
		}
	} else {
		b.successes = 0
		if b.failures < failureThreshold {
			b.failures++
		}
		if b.failures >= failureThreshold {
			b.state = "unhealthy"
		}
	}
}

type Pool struct {
	mu       sync.Mutex
	backends map[string]*Backend
	ordered  []*Backend
	cursors  map[string]int
}

// New requires config.Validate to have succeeded.
func New(configs []config.Upstream) *Pool {
	p := &Pool{backends: make(map[string]*Backend), cursors: make(map[string]int)}
	for _, c := range configs {
		u, _ := url.Parse(c.URL)
		h, _ := url.Parse(c.HealthPath)
		b := &Backend{ID: c.ID, URL: u, HealthURL: u.ResolveReference(h), state: "unknown"}
		p.backends[c.ID] = b
		p.ordered = append(p.ordered, b)
	}
	return p
}

func (p *Pool) All() []*Backend { return append([]*Backend(nil), p.ordered...) }

func (p *Pool) Pick(ids []string) *Backend {
	if len(ids) == 0 {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	key := strings.Join(ids, "\x00")
	start := p.cursors[key]
	for offset := 0; offset < len(ids); offset++ {
		index := (start + offset) % len(ids)
		if b := p.backends[ids[index]]; b != nil && b.Healthy() {
			p.cursors[key] = (index + 1) % len(ids)
			return b
		}
	}
	return nil
}

func (p *Pool) Ready(routes []config.Route) bool {
	for _, r := range routes {
		healthy := false
		for _, id := range r.Backends {
			if b := p.backends[id]; b != nil && b.Healthy() {
				healthy = true
				break
			}
		}
		if !healthy {
			return false
		}
	}
	return len(routes) > 0
}
