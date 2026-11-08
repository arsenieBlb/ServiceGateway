package metrics

import (
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestLatencyBucketsAreCumulativeAndIncludeBoundary(t *testing.T) {
	m := New([]string{"api"}, []string{"a"})
	for _, duration := range []time.Duration{0, time.Millisecond, time.Millisecond + time.Nanosecond, 5 * time.Millisecond, 10 * time.Millisecond, 6 * time.Second} {
		m.Record("api", 200, "ok", "a", duration)
	}
	routes, failures, backends := m.Snapshot()
	r := routes["api"]
	if r.Total != 6 || r.StatusClasses["2xx"] != 6 || r.Outcomes["ok"] != 6 || backends["a"] != 6 {
		t.Fatalf("totals do not reconcile: %+v %+v", r, backends)
	}
	want := map[string]uint64{"1ms": 2, "5ms": 4, "10ms": 5, "50ms": 5, "100ms": 5, "250ms": 5, "1s": 5, "5s": 5, "+Inf": 6}
	for bucket, count := range want {
		if r.LatencyBuckets[bucket] != count {
			t.Fatalf("bucket %s=%d expected%d", bucket, r.LatencyBuckets[bucket], count)
		}
	}
	for name, count := range failures {
		if count != 0 {
			t.Fatalf("healthy requests incremented failure %s=%d", name, count)
		}
	}
}

func TestFixedDimensionsAndFailureClassification(t *testing.T) {
	m := New([]string{"api"}, []string{"a"})
	for _, sample := range []struct {
		status  int
		outcome string
	}{{500, "ok"}, {502, "connect"}, {504, "timeout"}, {200, "partial"}, {0, "canceled"}} {
		m.Record("api", sample.status, sample.outcome, "a", time.Millisecond)
	}
	for i := 0; i < 1000; i++ {
		m.Record(fmt.Sprintf("/arbitrary/path/%d", i), 404, "not_found", fmt.Sprintf("arbitrary-backend-%d", i), time.Millisecond)
	}
	routes, failures, backends := m.Snapshot()
	if len(routes) != 2 || len(backends) != 1 || len(failures) != 4 {
		t.Fatal("identities expanded metric dimensions")
	}
	if routes[""].Total != 1000 || routes["api"].Total != 5 || backends["a"] != 5 {
		t.Fatalf("unknown ID accounting: %+v %+v", routes, backends)
	}
	if routes["api"].StatusClasses["5xx"] != 3 || routes["api"].StatusClasses["2xx"] != 1 || routes["api"].StatusClasses["none"] != 1 {
		t.Fatal("observed status was not preserved for partial/cancelled requests")
	}
	for _, failure := range []string{"connect", "timeout", "partial", "canceled"} {
		if failures[failure] != 1 {
			t.Fatalf("failure %s=%d expected1", failure, failures[failure])
		}
	}
}

func TestSnapshotOwnsCopiesWhileRequestsContinue(t *testing.T) {
	m := New([]string{"api"}, []string{"a"})
	const writers, perWriter = 8, 250
	start := make(chan struct{})
	var writes sync.WaitGroup
	for i := 0; i < writers; i++ {
		writes.Add(1)
		go func() {
			defer writes.Done()
			<-start
			for j := 0; j < perWriter; j++ {
				m.Record("api", 200, "ok", "a", 7*time.Millisecond)
			}
		}()
	}
	var snapshots sync.WaitGroup
	for i := 0; i < 3; i++ {
		snapshots.Add(1)
		go func() {
			defer snapshots.Done()
			<-start
			for j := 0; j < 100; j++ {
				routes, failures, backends := m.Snapshot()
				r := routes["api"]
				if r.Total != r.StatusClasses["2xx"] || r.Total != r.Outcomes["ok"] || r.Total != r.LatencyBuckets["+Inf"] || r.Total != backends["a"] {
					t.Errorf("inconsistent atomic snapshot: %+v %+v", r, backends)
					return
				}
				if _, err := json.Marshal(routes); err != nil {
					t.Error(err)
					return
				}
				// A consumer can mutate returned maps without racing or corrupting
				// the registry used by concurrent request handlers.
				r.StatusClasses["poison"] = 999
				r.Outcomes["poison"] = 999
				r.LatencyBuckets["poison"] = 999
				routes["poison"] = Route{}
				failures["poison"] = 999
				backends["poison"] = 999
			}
		}()
	}
	close(start)
	writes.Wait()
	snapshots.Wait()
	routes, failures, backends := m.Snapshot()
	want := uint64(writers * perWriter)
	if routes["api"].Total != want || backends["a"] != want || routes["api"].LatencyBuckets["10ms"] != want || routes["api"].LatencyBuckets["5ms"] != 0 {
		t.Fatalf("concurrent counts: %+v %+v", routes["api"], backends)
	}
	if len(routes) != 2 || len(failures) != 4 || len(backends) != 1 || len(routes["api"].StatusClasses) != 1 || len(routes["api"].Outcomes) != 1 {
		t.Fatal("snapshot mutation leaked into registry")
	}
}
