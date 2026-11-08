package main

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Rebuild phase totals and nearest-rank percentiles from individual operations.
// The wall-clock measurement windows remain recorded metadata.
func recalculate(r *result) {
	for i := range r.Phases {
		p := &r.Phases[i]
		p.Offered, p.Completed = 0, 0
		p.Statuses, p.Backends = map[string]int{}, map[string]int{}
		var latencies []float64
		for _, row := range r.Requests {
			if row.Phase != p.Name {
				continue
			}
			p.Offered++
			if row.Error == "" {
				p.Completed++
			}
			p.Statuses[strconv.Itoa(row.Status)]++
			if row.Backend != "" {
				p.Backends[row.Backend]++
			}
			latencies = append(latencies, row.LatencyMS)
		}
		sort.Float64s(latencies)
		percentile := func(q float64) float64 {
			if len(latencies) == 0 {
				return 0
			}
			return latencies[int(math.Ceil(q*float64(len(latencies))))-1]
		}
		p.P50MS, p.P95MS, p.P99MS = percentile(.50), percentile(.95), percentile(.99)
		p.OfferedPerSecond = float64(p.Offered) / p.DurationSeconds
		p.CompletedPerSecond = float64(p.Completed) / p.CompletionSeconds
	}
}

func sourceDigest() string {
	var paths []string
	_ = filepath.WalkDir(".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() && (d.Name() == ".git" || d.Name() == "bin" || d.Name() == "results") {
			return filepath.SkipDir
		}
		if !d.IsDir() && (strings.HasSuffix(path, ".go") || path == "go.mod" || path == "go.sum") {
			paths = append(paths, path)
		}
		return nil
	})
	sort.Strings(paths)
	hash := sha256.New()
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return "unavailable: " + err.Error()
		}
		fmt.Fprintf(hash, "%s\x00", filepath.ToSlash(path))
		hash.Write(data)
		hash.Write([]byte{0})
	}
	return hex.EncodeToString(hash.Sum(nil))
}

func report(r result) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Local demonstration measurements\n\nMeasured %s using %s on %s/%s, %d logical CPUs (%s), host `%s`.\n\nRevision: `%s`. Source SHA-256 (sorted Go files and go.mod/go.sum, each filename and content NUL-delimited): `%s`.\n\n", r.Started.Format("2006-01-02T15:04:05Z07:00"), r.Toolchain, r.OS, r.Architecture, r.CPUs, r.CPU, r.Hostname, r.Revision, r.SourceDigest)
	fmt.Fprintf(&b, "Command: `%s`. Run from the project root. See `raw.json` for exact configuration, every request (including errors), cumulative metrics samples and observed health transitions. Process logs and generated configuration are saved beside this report.\n\n", strings.Join(r.Command, " "))
	fmt.Fprintf(&b, "## Method\n\nThree real processes run on ephemeral loopback ports: two independent backend executables and the gateway. The driver hard-stops backend processes to exercise abrupt dependency loss, restarts the same addresses, and polls configured health states before settled failover/recovery phases. Graceful gateway shutdown is verified separately by integration tests; killing the demo processes during cleanup does not establish graceful shutdown.\n\nDirect and gateway baseline phases use the same driver, service handler, host, request path, target rate (%d/s), warm-up (%s), measured duration (%s) and %d repetitions. They run sequentially, so short measurements remain sensitive to scheduler and machine noise. No performance target is imposed.\n\n", r.BaselineRate, r.Warmup, r.Duration, r.Repetitions)
	fmt.Fprintf(&b, "All gateway limits remain enabled: active=%d, waiting=%d, wait=%s, body=%d bytes, per-peer refill=%g/s and burst=%d (chosen high enough not to bind in this demonstration), request deadline=%s, response-header timeout=%s. Slow phase uses 600ms backend delay at 5/s. Overload uses 150ms delay at %d/s. Response bodies are fully consumed by the driver so completion latency includes them.\n\n", r.ActiveLimit, r.WaitingLimit, r.WaitLimit, r.Config.Limits.BodyBytes, r.Config.Rate.Refill, r.Config.Rate.Burst, r.RequestDeadline, time.Duration(r.Config.Timeouts.ResponseHeader), r.OverloadRate)
	b.WriteString("Each offered request is one driver operation; completed means an HTTP response was received and read without a transport/body error, including gateway 4xx/5xx responses. Offered/s = offered count / configured phase seconds. Completed/s = completed count / actual elapsed seconds through the last completion. Latency percentiles use nearest rank `ceil(q*N)-1` over all operations, including rejected and failed requests. Status 0 identifies operations without an HTTP response. Raw per-request data allows alternate calculations.\n\n")
	b.WriteString("## Results\n\n| Phase | Offered | Completed | Offered/s | Completed/s | p50 ms | p95 ms | p99 ms | Status counts | Backend counts |\n| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |\n")
	for _, p := range r.Phases {
		fmt.Fprintf(&b, "| %s | %d | %d | %.2f | %.2f | %.3f | %.3f | %.3f | %s | %s |\n", p.Name, p.Offered, p.Completed, p.OfferedPerSecond, p.CompletedPerSecond, p.P50MS, p.P95MS, p.P99MS, counts(p.Statuses), counts(p.Backends))
	}
	fmt.Fprintf(&b, "\nGateway cumulative maxima observed in metrics: active **%d/%d**, waiting **%d/%d**. The maxima are maintained by admission accounting, not estimated solely from periodic sampling. This bounds proxied upstream work and waiting admission; it does not bound all client connections or process memory.\n\n", r.MaxActive, r.ActiveLimit, r.MaxWaiting, r.WaitingLimit)
	b.WriteString("| Backend transition | Observed delay from process action (ms) |\n| --- | ---: |\n")
	for _, t := range r.Transitions {
		fmt.Fprintf(&b, "| %s → %s | %.3f |\n", t.Backend, t.Expected, t.DelayMS)
	}
	b.WriteString("\nHealth times include process scheduling and polling delay; they are measurements of this run rather than universal detection guarantees. Detection-window requests can legitimately fail before failed-backend health transitions.\n\nChecks:\n\n")
	keys := make([]string, 0, len(r.Checks))
	for k := range r.Checks {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Fprintf(&b, "- `%s`: %t\n", k, r.Checks[k])
	}
	b.WriteString("\nThese are local demonstration measurements, not production capacity claims. The load driver itself uses one goroutine per offered operation and a bounded maximum offered rate flag; gateway admission bounds apply to gateway work, not driver resources.\n")
	return b.String()
}
func counts(m map[string]int) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, 0, len(keys))
	for _, k := range keys {
		parts = append(parts, fmt.Sprintf("%s=%d", k, m[k]))
	}
	if len(parts) == 0 {
		return "—"
	}
	return strings.Join(parts, ", ")
}
