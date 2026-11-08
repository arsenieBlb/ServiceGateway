// demo launches real local processes and saves raw, reproducible measurements.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"service-gateway/internal/config"
)

type observation struct {
	Phase     string    `json:"phase"`
	Sequence  int       `json:"sequence"`
	Start     time.Time `json:"start"`
	LatencyMS float64   `json:"latency_ms"`
	Status    int       `json:"status"`
	Backend   string    `json:"backend,omitempty"`
	Error     string    `json:"error,omitempty"`
}
type metricSample struct {
	At   time.Time      `json:"at"`
	Data map[string]any `json:"data"`
}
type transition struct {
	Backend, Expected string
	Action, Observed  time.Time
	DelayMS           float64
}
type phaseSummary struct {
	Name                                                                     string `json:"name"`
	Offered, Completed                                                       int
	DurationSeconds, CompletionSeconds, OfferedPerSecond, CompletedPerSecond float64
	P50MS, P95MS, P99MS                                                      float64
	Statuses                                                                 map[string]int
	Backends                                                                 map[string]int
}
type result struct {
	Started                                                            time.Time `json:"started"`
	Toolchain, OS, Architecture, Hostname, CPU, Revision, SourceDigest string
	CPUs                                                               int
	Warmup, Duration                                                   string
	Repetitions, BaselineRate, OverloadRate, ActiveLimit, WaitingLimit int
	WaitLimit, RequestDeadline                                         string
	Command                                                            []string
	Config                                                             config.Config
	Requests                                                           []observation  `json:"requests"`
	Metrics                                                            []metricSample `json:"metrics"`
	Transitions                                                        []transition   `json:"transitions"`
	Phases                                                             []phaseSummary `json:"phases"`
	MaxActive, MaxWaiting                                              int
	Checks                                                             map[string]bool
}
type process struct {
	command *exec.Cmd
	done    chan error
	log     *os.File
}

func startProcess(binary string, logPath string, args ...string) (*process, error) {
	f, err := os.Create(logPath)
	if err != nil {
		return nil, err
	}
	c := exec.Command(binary, args...)
	c.Stdout = f
	c.Stderr = f
	if err = c.Start(); err != nil {
		f.Close()
		return nil, err
	}
	p := &process{command: c, done: make(chan error, 1), log: f}
	go func() { p.done <- c.Wait(); f.Close() }()
	return p, nil
}
func (p *process) stop() {
	if p == nil {
		return
	}
	_ = p.command.Process.Kill()
	select {
	case <-p.done:
	case <-time.After(3 * time.Second):
	}
}
func freeAddress() (string, error) {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		return "", e
	}
	a := l.Addr().String()
	e = l.Close()
	return a, e
}
func require(err error) {
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func getMetrics(c *http.Client, url string) (map[string]any, error) {
	r, e := c.Get(url)
	if e != nil {
		return nil, e
	}
	defer r.Body.Close()
	var data map[string]any
	e = json.NewDecoder(r.Body).Decode(&data)
	return data, e
}
func await(ctx context.Context, condition func() bool) error {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if condition() {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
func backendState(data map[string]any, id string) string {
	items, _ := data["upstreams"].([]any)
	for _, item := range items {
		b, _ := item.(map[string]any)
		if b["id"] == id {
			v, _ := b["state"].(string)
			return v
		}
	}
	return ""
}
func fieldInt(data map[string]any, key string) int {
	for k, v := range data {
		if strings.EqualFold(strings.ReplaceAll(k, "_", ""), strings.ReplaceAll(key, "_", "")) {
			n, _ := v.(float64)
			return int(n)
		}
	}
	return 0
}
func phase(client *http.Client, name, url string, duration time.Duration, rate int) ([]observation, phaseSummary) {
	start := time.Now()
	deadline := start.Add(duration)
	interval := time.Second / time.Duration(rate)
	var mu sync.Mutex
	var wg sync.WaitGroup
	rows := make([]observation, 0, int(duration.Seconds()*float64(rate))+1)
	sequence := 0
	for next := start; next.Before(deadline); next = next.Add(interval) {
		if d := time.Until(next); d > 0 {
			timer := time.NewTimer(d)
			<-timer.C
		}
		sequence++
		n := sequence
		wg.Add(1)
		go func() {
			defer wg.Done()
			at := time.Now()
			row := observation{Phase: name, Sequence: n, Start: at}
			resp, err := client.Get(url)
			if err != nil {
				row.Error = err.Error()
			} else {
				row.Status = resp.StatusCode
				row.Backend = resp.Header.Get("X-Backend-ID")
				_, e := io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if e != nil {
					row.Error = e.Error()
				}
			}
			row.LatencyMS = float64(time.Since(at)) / float64(time.Millisecond)
			mu.Lock()
			rows = append(rows, row)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(start).Seconds()
	sort.Slice(rows, func(i, j int) bool { return rows[i].Sequence < rows[j].Sequence })
	s := phaseSummary{Name: name, Offered: sequence, DurationSeconds: duration.Seconds(), CompletionSeconds: elapsed, Statuses: map[string]int{}, Backends: map[string]int{}}
	latencies := make([]float64, 0, len(rows))
	for _, r := range rows {
		if r.Error == "" {
			s.Completed++
		}
		s.Statuses[strconv.Itoa(r.Status)]++
		if r.Backend != "" {
			s.Backends[r.Backend]++
		}
		latencies = append(latencies, r.LatencyMS)
	}
	sort.Float64s(latencies)
	p := func(q float64) float64 {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(math.Ceil(float64(len(latencies))*q))-1]
	}
	s.P50MS = p(.50)
	s.P95MS = p(.95)
	s.P99MS = p(.99)
	s.OfferedPerSecond = float64(sequence) / duration.Seconds()
	s.CompletedPerSecond = float64(s.Completed) / elapsed
	return rows, s
}

func run() error {
	defaultSuffix := ""
	if runtime.GOOS == "windows" {
		defaultSuffix = ".exe"
	}
	gatewayBin := flag.String("gateway-bin", filepath.Join("bin", "gateway"+defaultSuffix), "built gateway executable")
	backendBin := flag.String("backend-bin", filepath.Join("bin", "backend"+defaultSuffix), "built backend executable")
	out := flag.String("out", "demo/results/latest", "result directory")
	reportFrom := flag.String("report-from", "", "recalculate report from an existing raw.json without launching processes")
	duration := flag.Duration("duration", time.Second, "duration per measured phase")
	warmup := flag.Duration("warmup", 300*time.Millisecond, "warm-up per healthy baseline")
	repetitions := flag.Int("repetitions", 3, "direct and gateway baseline repetitions")
	rate := flag.Int("rate", 50, "offered requests/second for baselines")
	overload := flag.Int("overload-rate", 1000, "offered requests/second with 150ms service delay")
	flag.Parse()
	if *reportFrom != "" {
		data, err := os.ReadFile(*reportFrom)
		if err != nil {
			return err
		}
		var saved result
		if err := json.Unmarshal(data, &saved); err != nil {
			return err
		}
		recalculate(&saved)
		if err := os.MkdirAll(*out, 0755); err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(*out, "report.md"), []byte(report(saved)), 0644)
	}
	if *duration <= 0 || *warmup < 0 || *repetitions < 1 || *rate < 1 || *rate > 10000 || *overload < 1 || *overload > 10000 {
		return errors.New("positive duration/repetitions and rates 1..10000 required")
	}
	if err := os.MkdirAll(*out, 0755); err != nil {
		return err
	}
	gatewayAbs, e := filepath.Abs(*gatewayBin)
	if e != nil {
		return e
	}
	backendAbs, e := filepath.Abs(*backendBin)
	if e != nil {
		return e
	}
	a, e := freeAddress()
	if e != nil {
		return e
	}
	b, e := freeAddress()
	if e != nil {
		return e
	}
	front, e := freeAddress()
	if e != nil {
		return e
	}
	admin, e := freeAddress()
	if e != nil {
		return e
	}
	cfg := config.Default()
	cfg.Listen = front
	cfg.AdminListen = admin
	cfg.Upstreams = []config.Upstream{{ID: "a", URL: "http://" + a, HealthPath: "/healthz"}, {ID: "b", URL: "http://" + b, HealthPath: "/healthz"}}
	cfg.Routes = []config.Route{{ID: "api", Prefix: "/api", Backends: []string{"a", "b"}}}
	cfg.Health.Interval = config.Duration(50 * time.Millisecond)
	cfg.Health.Timeout = config.Duration(50 * time.Millisecond)
	cfg.Health.SuccessThreshold = 2
	cfg.Health.FailureThreshold = 2
	cfg.Limits.Active = 4
	cfg.Limits.Waiting = 8
	cfg.Limits.Wait = config.Duration(50 * time.Millisecond)
	cfg.Rate.Refill = 100000
	cfg.Rate.Burst = 100000
	cfg.Timeouts.Request = config.Duration(300 * time.Millisecond)
	cfg.Timeouts.ResponseHeader = config.Duration(250 * time.Millisecond)
	encoded, e := json.MarshalIndent(cfg, "", "  ")
	if e != nil {
		return e
	}
	configPath := filepath.Join(*out, "config.json")
	if e = os.WriteFile(configPath, encoded, 0644); e != nil {
		return e
	}
	pa, e := startProcess(backendAbs, filepath.Join(*out, "backend-a.log"), "-listen", a, "-id", "a")
	if e != nil {
		return e
	}
	defer func() { pa.stop() }()
	pb, e := startProcess(backendAbs, filepath.Join(*out, "backend-b.log"), "-listen", b, "-id", "b")
	if e != nil {
		return e
	}
	defer func() { pb.stop() }()
	pg, e := startProcess(gatewayAbs, filepath.Join(*out, "gateway.log"), "-config", configPath)
	if e != nil {
		return e
	}
	defer pg.stop()
	transport := &http.Transport{MaxIdleConns: 256, MaxIdleConnsPerHost: 128, MaxConnsPerHost: 256, IdleConnTimeout: 5 * time.Second}
	client := &http.Client{Transport: transport, Timeout: 2 * time.Second}
	defer transport.CloseIdleConnections()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if e = await(ctx, func() bool {
		m, e := getMetrics(client, "http://"+admin+"/metrics")
		return e == nil && backendState(m, "a") == "healthy" && backendState(m, "b") == "healthy"
	}); e != nil {
		return fmt.Errorf("gateway startup/health: %w (see %s)", e, *out)
	}
	hostname, _ := os.Hostname()
	revision := "unversioned workspace"
	if v, e := exec.Command("git", "rev-parse", "HEAD").Output(); e == nil {
		revision = strings.TrimSpace(string(v)) + " (check source digest; workspace may be uncommitted)"
	}
	cpu := "unavailable"
	if data, e := os.ReadFile("/proc/cpuinfo"); e == nil {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "model name") {
				cpu = strings.TrimSpace(strings.SplitN(line, ":", 2)[1])
				break
			}
		}
	} else {
		cpu = os.Getenv("PROCESSOR_IDENTIFIER")
	}
	r := result{Started: time.Now().UTC(), Toolchain: runtime.Version(), OS: runtime.GOOS, Architecture: runtime.GOARCH, Hostname: hostname, CPU: cpu, CPUs: runtime.NumCPU(), Revision: revision, SourceDigest: sourceDigest(), Warmup: warmup.String(), Duration: duration.String(), Repetitions: *repetitions, BaselineRate: *rate, OverloadRate: *overload, ActiveLimit: 4, WaitingLimit: 8, WaitLimit: "50ms", RequestDeadline: "300ms", Command: os.Args, Config: cfg, Checks: map[string]bool{}}
	var metricsMu sync.Mutex
	stopMetrics := make(chan struct{})
	metricsDone := make(chan struct{})
	go func() {
		defer close(metricsDone)
		ticker := time.NewTicker(10 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-stopMetrics:
				return
			case <-ticker.C:
				m, e := getMetrics(client, "http://"+admin+"/metrics")
				if e != nil {
					continue
				}
				metricsMu.Lock()
				r.Metrics = append(r.Metrics, metricSample{At: time.Now().UTC(), Data: m})
				ad, _ := m["admission"].(map[string]any)
				active, waiting := fieldInt(ad, "MaxActive"), fieldInt(ad, "MaxWaiting")
				if active > r.MaxActive {
					r.MaxActive = active
				}
				if waiting > r.MaxWaiting {
					r.MaxWaiting = waiting
				}
				metricsMu.Unlock()
			}
		}
	}()
	defer func() {
		select {
		case <-stopMetrics:
		default:
			close(stopMetrics)
		}
		<-metricsDone
	}()
	doPhase := func(name, url string, d time.Duration, n int) {
		fmt.Fprintln(os.Stderr, "measuring", name)
		rows, summary := phase(client, name, url, d, n)
		r.Requests = append(r.Requests, rows...)
		r.Phases = append(r.Phases, summary)
	}
	for i := 1; i <= *repetitions; i++ {
		suffix := strconv.Itoa(i)
		if *warmup > 0 {
			doPhase("warmup-direct-"+suffix, "http://"+a+"/api", *warmup, *rate)
		}
		doPhase("direct-"+suffix, "http://"+a+"/api", *duration, *rate)
		if *warmup > 0 {
			doPhase("warmup-gateway-"+suffix, "http://"+front+"/api", *warmup, *rate)
		}
		doPhase("gateway-"+suffix, "http://"+front+"/api", *duration, *rate)
	}
	waitState := func(id, state string, action time.Time) error {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		e := await(ctx, func() bool {
			m, e := getMetrics(client, "http://"+admin+"/metrics")
			return e == nil && backendState(m, id) == state
		})
		if e == nil {
			r.Transitions = append(r.Transitions, transition{Backend: id, Expected: state, Action: action, Observed: time.Now().UTC(), DelayMS: float64(time.Since(action)) / float64(time.Millisecond)})
		}
		return e
	}
	stopped := time.Now().UTC()
	pa.stop()
	pa = nil
	transitionDone := make(chan error, 1)
	go func() { transitionDone <- waitState("a", "unhealthy", stopped) }()
	// Capture the detection window as well as the settled failover state.
	doPhase("backend-a-detection", "http://"+front+"/api", 300*time.Millisecond, *rate)
	if e = <-transitionDone; e != nil {
		return e
	}
	doPhase("backend-a-down", "http://"+front+"/api", *duration, *rate)
	restarted := time.Now().UTC()
	pa, e = startProcess(backendAbs, filepath.Join(*out, "backend-a-recovered.log"), "-listen", a, "-id", "a")
	if e != nil {
		return e
	}
	if e = waitState("a", "healthy", restarted); e != nil {
		return e
	}
	doPhase("recovered", "http://"+front+"/api", *duration, *rate)
	stopped = time.Now().UTC()
	pa.stop()
	pa = nil
	pb.stop()
	pb = nil
	if e = waitState("a", "unhealthy", stopped); e != nil {
		return e
	}
	if e = waitState("b", "unhealthy", stopped); e != nil {
		return e
	}
	doPhase("all-down", "http://"+front+"/api", *duration, *rate)
	restarted = time.Now().UTC()
	pa, e = startProcess(backendAbs, filepath.Join(*out, "backend-a-final.log"), "-listen", a, "-id", "a")
	if e != nil {
		return e
	}
	pb, e = startProcess(backendAbs, filepath.Join(*out, "backend-b-recovered.log"), "-listen", b, "-id", "b")
	if e != nil {
		return e
	}
	if e = waitState("a", "healthy", restarted); e != nil {
		return e
	}
	if e = waitState("b", "healthy", restarted); e != nil {
		return e
	}
	doPhase("slow-deadline", "http://"+front+"/api?delay=600ms", *duration, 5)
	doPhase("overload", "http://"+front+"/api?delay=150ms", *duration, *overload)
	close(stopMetrics)
	<-metricsDone
	r.Checks["active_bound"] = r.MaxActive <= r.ActiveLimit
	r.Checks["waiting_bound"] = r.MaxWaiting <= r.WaitingLimit
	for _, p := range r.Phases {
		switch p.Name {
		case "backend-a-down":
			r.Checks["failover_only_b"] = p.Completed == p.Offered && p.Statuses["200"] == p.Offered && p.Backends["b"] == p.Offered
		case "recovered":
			r.Checks["recovered_distribution"] = p.Backends["a"] > 0 && p.Backends["b"] > 0
		case "all-down":
			r.Checks["all_down_503"] = p.Statuses["503"] == p.Offered
		case "slow-deadline":
			r.Checks["slow_504"] = p.Statuses["504"] == p.Offered
		case "overload":
			r.Checks["overload_rejects"] = p.Statuses["503"] > 0
		}
	}
	data, e := json.MarshalIndent(r, "", "  ")
	if e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(*out, "raw.json"), data, 0644); e != nil {
		return e
	}
	if e = os.WriteFile(filepath.Join(*out, "report.md"), []byte(report(r)), 0644); e != nil {
		return e
	}
	fmt.Printf("Saved %d requests, %d metric snapshots to %s\n", len(r.Requests), len(r.Metrics), *out)
	for name, ok := range r.Checks {
		if !ok {
			return fmt.Errorf("demonstration check failed: %s (raw results saved)", name)
		}
	}
	return nil
}

func main() { require(run()) }
