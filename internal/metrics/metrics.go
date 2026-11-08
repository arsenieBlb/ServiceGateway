// Package metrics keeps only configured route/backend identities as dimensions.
package metrics

import (
	"sync"
	"time"
)

var bounds = []time.Duration{time.Millisecond, 5 * time.Millisecond, 10 * time.Millisecond, 50 * time.Millisecond, 100 * time.Millisecond, 250 * time.Millisecond, time.Second, 5 * time.Second}

type Route struct {
	Total          uint64            `json:"total"`
	StatusClasses  map[string]uint64 `json:"status_classes"`
	Outcomes       map[string]uint64 `json:"outcomes"`
	LatencyBuckets map[string]uint64 `json:"latency_buckets"`
}

type Registry struct {
	mu       sync.Mutex
	routes   map[string]*Route
	failures map[string]uint64
	backends map[string]uint64
}

func New(routeIDs, backendIDs []string) *Registry {
	m := &Registry{routes: make(map[string]*Route), failures: map[string]uint64{"connect": 0, "timeout": 0, "partial": 0, "canceled": 0}, backends: make(map[string]uint64)}
	for _, id := range append(routeIDs, "") {
		m.routes[id] = &Route{StatusClasses: map[string]uint64{}, Outcomes: map[string]uint64{}, LatencyBuckets: map[string]uint64{}}
	}
	for _, id := range backendIDs {
		m.backends[id] = 0
	}
	return m
}

func (m *Registry) Record(route string, status int, outcome, backend string, latency time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	r, ok := m.routes[route]
	if !ok {
		r = m.routes[""]
	}
	r.Total++
	class := "none"
	if status >= 100 && status < 600 {
		class = string(rune('0'+status/100)) + "xx"
	}
	r.StatusClasses[class]++
	r.Outcomes[outcome]++
	for _, b := range bounds {
		if latency <= b {
			r.LatencyBuckets[b.String()]++
		}
	}
	r.LatencyBuckets["+Inf"]++
	if _, ok := m.backends[backend]; ok {
		m.backends[backend]++
	}
	if _, ok := m.failures[outcome]; ok {
		m.failures[outcome]++
	}
}

// Snapshot owns its copies so JSON encoding never races with requests.
func (m *Registry) Snapshot() (map[string]Route, map[string]uint64, map[string]uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	routes := make(map[string]Route)
	for id, r := range m.routes {
		routes[id] = Route{r.Total, clone(r.StatusClasses), clone(r.Outcomes), clone(r.LatencyBuckets)}
	}
	return routes, clone(m.failures), clone(m.backends)
}
func clone(src map[string]uint64) map[string]uint64 {
	dst := make(map[string]uint64, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
