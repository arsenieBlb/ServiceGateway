package upstreams

import (
	"service-gateway/internal/config"
	"sync"
	"testing"
)

func testPool() *Pool {
	return New([]config.Upstream{{ID: "a", URL: "http://localhost:1", HealthPath: "/healthz"}, {ID: "b", URL: "http://localhost:2", HealthPath: "/healthz"}})
}

func TestThresholdsAndRecovery(t *testing.T) {
	b := testPool().All()[0]
	if b.State() != "unknown" || b.Healthy() {
		t.Fatal("initial state must be unknown/unready")
	}
	b.Observe(true, 2, 2)
	if b.Healthy() {
		t.Fatal("premature healthy")
	}
	b.Observe(false, 2, 2)
	if b.State() != "unknown" {
		t.Fatal("premature unhealthy")
	}
	b.Observe(false, 2, 2)
	if b.State() != "unhealthy" {
		t.Fatal("failure threshold")
	}
	b.Observe(true, 2, 2)
	if b.Healthy() {
		t.Fatal("premature recovery")
	}
	b.Observe(true, 2, 2)
	if !b.Healthy() {
		t.Fatal("recovery threshold")
	}
	b.Observe(false, 2, 2)
	if !b.Healthy() {
		t.Fatal("flapped after one failure")
	}
	b.Observe(true, 2, 2)
	b.Observe(false, 2, 2)
	if !b.Healthy() {
		t.Fatal("streak not reset")
	}
	b.Observe(false, 2, 2)
	if b.Healthy() {
		t.Fatal("failure threshold")
	}
}

func TestRoundRobinAndReadiness(t *testing.T) {
	p := testPool()
	ids := []string{"a", "b"}
	routes := []config.Route{{ID: "one", Backends: ids}, {ID: "two", Backends: []string{"b"}}}
	if p.Ready(routes) || p.Pick(ids) != nil {
		t.Fatal("unknown selected")
	}
	p.All()[0].Observe(true, 1, 1)
	if p.Ready(routes) {
		t.Fatal("every route must be healthy")
	}
	if b := p.Pick(ids); b == nil || b.ID != "a" {
		t.Fatal("healthy backend skipped")
	}
	p.All()[1].Observe(true, 1, 1)
	if !p.Ready(routes) {
		t.Fatal("healthy routes not ready")
	}
	for i := 0; i < 10; i++ {
		want := "b"
		if i%2 == 1 {
			want = "a"
		}
		if b := p.Pick(ids); b == nil || b.ID != want {
			t.Fatalf("round robin %d got %v want %s", i, b, want)
		}
	}
	p.All()[0].Observe(false, 1, 1)
	p.All()[1].Observe(false, 1, 1)
	if p.Ready(routes) || p.Pick(ids) != nil {
		t.Fatal("unhealthy selected")
	}
	p.All()[0].Observe(true, 1, 1)
	if p.Pick(ids) == nil {
		t.Fatal("recovery failed")
	}
}

func TestConcurrentSelection(t *testing.T) {
	p := testPool()
	for _, b := range p.All() {
		b.Observe(true, 1, 1)
	}
	counts := map[string]int{}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				b := p.Pick([]string{"a", "b"})
				mu.Lock()
				counts[b.ID]++
				mu.Unlock()
				_ = b.State()
			}
		}()
	}
	wg.Wait()
	if counts["a"] != 5000 || counts["b"] != 5000 {
		t.Fatalf("distribution %+v", counts)
	}
}
