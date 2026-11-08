// Package config loads and validates the static gateway configuration.
package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type Duration time.Duration

func (d *Duration) UnmarshalJSON(data []byte) error {
	var value string
	if err := json.Unmarshal(data, &value); err != nil {
		return fmt.Errorf("duration must be a string such as 250ms: %w", err)
	}
	parsed, err := time.ParseDuration(value)
	if err != nil {
		return fmt.Errorf("invalid duration %q: %w", value, err)
	}
	*d = Duration(parsed)
	return nil
}

func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

type Config struct {
	Listen      string     `json:"listen"`
	AdminListen string     `json:"admin_listen"`
	Routes      []Route    `json:"routes"`
	Upstreams   []Upstream `json:"upstreams"`
	Health      Health     `json:"health"`
	Limits      Limits     `json:"limits"`
	Rate        Rate       `json:"rate"`
	Timeouts    Timeouts   `json:"timeouts"`
}

type Route struct {
	ID       string   `json:"id"`
	Prefix   string   `json:"prefix"`
	Backends []string `json:"backends"`
}

type Upstream struct {
	ID         string `json:"id"`
	URL        string `json:"url"`
	HealthPath string `json:"health_path"`
}

type Health struct {
	Interval         Duration `json:"interval"`
	Timeout          Duration `json:"timeout"`
	SuccessThreshold int      `json:"success_threshold"`
	FailureThreshold int      `json:"failure_threshold"`
}

type Limits struct {
	Active         int      `json:"active"`
	Waiting        int      `json:"waiting"`
	Wait           Duration `json:"wait"`
	BodyBytes      int64    `json:"body_bytes"`
	MaxHeaderBytes int      `json:"max_header_bytes"`
}

type Rate struct {
	Refill     float64  `json:"refill"`
	Burst      int      `json:"burst"`
	MaxEntries int      `json:"max_entries"`
	IdleTTL    Duration `json:"idle_ttl"`
}

type Timeouts struct {
	ReadHeader      Duration `json:"read_header"`
	ReadBody        Duration `json:"read_body"`
	Idle            Duration `json:"idle"`
	Connect         Duration `json:"connect"`
	ResponseHeader  Duration `json:"response_header"`
	Request         Duration `json:"request"`
	Shutdown        Duration `json:"shutdown"`
	MaxConnsPerHost int      `json:"max_conns_per_host"`
}

func Default() Config {
	return Config{
		Listen: "127.0.0.1:8080", AdminListen: "127.0.0.1:9090",
		Health:   Health{Duration(time.Second), Duration(500 * time.Millisecond), 2, 2},
		Limits:   Limits{32, 64, Duration(250 * time.Millisecond), 1 << 20, 16 << 10},
		Rate:     Rate{100, 200, 10000, Duration(5 * time.Minute)},
		Timeouts: Timeouts{Duration(2 * time.Second), Duration(5 * time.Second), Duration(30 * time.Second), Duration(time.Second), Duration(2 * time.Second), Duration(5 * time.Second), Duration(5 * time.Second), 64},
	}
}

func Load(r io.Reader) (Config, error) {
	c := Default()
	data, err := io.ReadAll(io.LimitReader(r, (1<<20)+1))
	if err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	if len(data) > 1<<20 {
		return Config{}, fmt.Errorf("configuration exceeds 1 MiB")
	}
	d := json.NewDecoder(bytes.NewReader(data))
	d.DisallowUnknownFields()
	if err := d.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("configuration: %w", err)
	}
	var extra any
	if err := d.Decode(&extra); err != io.EOF {
		return Config{}, fmt.Errorf("configuration: expected one JSON object")
	}
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

var identifier = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,63}$`)

func validListener(address string) error {
	host, port, err := net.SplitHostPort(address)
	if err != nil {
		return err
	}
	n, err := strconv.Atoi(port)
	if err != nil || n < 0 || n > 65535 {
		return fmt.Errorf("invalid port")
	}
	ip := net.ParseIP(host)
	if host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return fmt.Errorf("version one requires a loopback host")
	}
	return nil
}

func (c Config) Validate() error {
	for name, address := range map[string]string{"listen": c.Listen, "admin_listen": c.AdminListen} {
		if err := validListener(address); err != nil {
			return fmt.Errorf("%s %q: %w", name, address, err)
		}
	}
	if c.Listen == c.AdminListen && !strings.HasSuffix(c.Listen, ":0") {
		return fmt.Errorf("listen and admin_listen must differ")
	}
	if len(c.Routes) == 0 || len(c.Routes) > 1024 {
		return fmt.Errorf("routes: require 1..1024 routes")
	}
	if len(c.Upstreams) == 0 || len(c.Upstreams) > 1024 {
		return fmt.Errorf("upstreams: require 1..1024 upstreams")
	}
	backends := make(map[string]bool)
	for _, b := range c.Upstreams {
		if !identifier.MatchString(b.ID) {
			return fmt.Errorf("upstream id %q must be 1..64 letters, numbers, underscores or hyphens", b.ID)
		}
		if backends[b.ID] {
			return fmt.Errorf("duplicate upstream id %q", b.ID)
		}
		backends[b.ID] = true
		u, err := url.Parse(b.URL)
		if err != nil || u == nil || (u.Scheme != "http" && u.Scheme != "https") || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || (u.Path != "" && u.Path != "/") || u.RawPath != "" {
			return fmt.Errorf("upstream %q: URL must be an absolute HTTP/HTTPS origin without credentials, query, fragment or path", b.ID)
		}
		if port := u.Port(); port != "" {
			n, err := strconv.Atoi(port)
			if err != nil || n < 1 || n > 65535 {
				return fmt.Errorf("upstream %q: invalid port", b.ID)
			}
		}
		if strings.HasSuffix(u.Host, ":") || (strings.HasPrefix(u.Host, "[") && net.ParseIP(strings.Split(u.Hostname(), "%")[0]) == nil) {
			return fmt.Errorf("upstream %q: malformed host or port", b.ID)
		}
		h, err := url.Parse(b.HealthPath)
		if err != nil || h == nil || !strings.HasPrefix(b.HealthPath, "/") || strings.HasPrefix(b.HealthPath, "//") || h.IsAbs() || h.Host != "" || h.RawQuery != "" || h.ForceQuery || h.Fragment != "" {
			return fmt.Errorf("upstream %q: health_path must be an absolute path without query or fragment", b.ID)
		}
	}
	routes, prefixes := make(map[string]bool), make(map[string]bool)
	for _, r := range c.Routes {
		if !identifier.MatchString(r.ID) {
			return fmt.Errorf("route id %q is invalid", r.ID)
		}
		if routes[r.ID] {
			return fmt.Errorf("duplicate route id %q", r.ID)
		}
		routes[r.ID] = true
		if !strings.HasPrefix(r.Prefix, "/") || strings.ContainsAny(r.Prefix, "?#%\\") || strings.Contains(r.Prefix, "//") || strings.Contains(r.Prefix, "/../") || strings.Contains(r.Prefix, "/./") || strings.HasSuffix(r.Prefix, "/..") || strings.HasSuffix(r.Prefix, "/.") || (r.Prefix != "/" && strings.HasSuffix(r.Prefix, "/")) {
			return fmt.Errorf("route %q: prefix must be a clean absolute path with no trailing slash except /", r.ID)
		}
		if prefixes[r.Prefix] {
			return fmt.Errorf("duplicate route prefix %q", r.Prefix)
		}
		prefixes[r.Prefix] = true
		if len(r.Backends) == 0 {
			return fmt.Errorf("route %q has no backends", r.ID)
		}
		seen := make(map[string]bool)
		for _, id := range r.Backends {
			if !backends[id] {
				return fmt.Errorf("route %q references unknown backend %q", r.ID, id)
			}
			if seen[id] {
				return fmt.Errorf("route %q repeats backend %q", r.ID, id)
			}
			seen[id] = true
		}
	}
	for name, value := range map[string]Duration{
		"health.interval": c.Health.Interval, "health.timeout": c.Health.Timeout, "limits.wait": c.Limits.Wait, "rate.idle_ttl": c.Rate.IdleTTL,
		"timeouts.read_header": c.Timeouts.ReadHeader, "timeouts.read_body": c.Timeouts.ReadBody, "timeouts.idle": c.Timeouts.Idle, "timeouts.connect": c.Timeouts.Connect, "timeouts.response_header": c.Timeouts.ResponseHeader, "timeouts.request": c.Timeouts.Request, "timeouts.shutdown": c.Timeouts.Shutdown,
	} {
		if value <= 0 || time.Duration(value) > 24*time.Hour {
			return fmt.Errorf("%s must be greater than zero and at most 24h", name)
		}
	}
	if c.Health.SuccessThreshold < 1 || c.Health.SuccessThreshold > 10000 || c.Health.FailureThreshold < 1 || c.Health.FailureThreshold > 10000 {
		return fmt.Errorf("health thresholds must be 1..10000")
	}
	if c.Limits.Active < 1 || c.Limits.Active > 100000 || c.Limits.Waiting < 0 || c.Limits.Waiting > 100000 {
		return fmt.Errorf("limits: active must be 1..100000 and waiting 0..100000")
	}
	if c.Limits.BodyBytes < 1 || c.Limits.BodyBytes > 64<<20 {
		return fmt.Errorf("limits.body_bytes must be 1..67108864")
	}
	if c.Limits.MaxHeaderBytes < 1024 || c.Limits.MaxHeaderBytes > 1<<20 {
		return fmt.Errorf("limits.max_header_bytes must be 1024..1048576")
	}
	if math.IsNaN(c.Rate.Refill) || math.IsInf(c.Rate.Refill, 0) || c.Rate.Refill <= 0 || c.Rate.Refill > 1e9 || c.Rate.Burst < 1 || c.Rate.Burst > 1000000000 || c.Rate.MaxEntries < 1 || c.Rate.MaxEntries > 1000000 {
		return fmt.Errorf("rate: positive finite refill <=1e9, burst 1..1e9 and max_entries 1..1000000 required")
	}
	if c.Timeouts.MaxConnsPerHost < 1 || c.Timeouts.MaxConnsPerHost > 100000 {
		return fmt.Errorf("timeouts.max_conns_per_host must be 1..100000")
	}
	return nil
}
