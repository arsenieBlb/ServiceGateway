package config

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func valid() Config {
	c := Default()
	c.Routes = []Route{{ID: "api", Prefix: "/api", Backends: []string{"one"}}}
	c.Upstreams = []Upstream{{ID: "one", URL: "http://127.0.0.1:8081", HealthPath: "/healthz"}}
	return c
}

func TestLoadDefaultsAndRoundTrip(t *testing.T) {
	raw := `{"routes":[{"id":"api","prefix":"/api","backends":["one"]}],"upstreams":[{"id":"one","url":"http://127.0.0.1:8081","health_path":"/healthz"}]}`
	c, err := Load(strings.NewReader(raw))
	if err != nil {
		t.Fatal(err)
	}
	if c.Limits.Active != 32 || c.Limits.Waiting != 64 || c.Limits.BodyBytes != 1<<20 || time.Duration(c.Limits.Wait) != 250*time.Millisecond {
		t.Fatalf("defaults: %+v", c.Limits)
	}
	b, err := json.Marshal(c)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := Load(strings.NewReader(string(b))); err != nil {
		t.Fatal(err)
	}
}

func TestLoadRejectsInvalidJSON(t *testing.T) {
	for _, raw := range []string{`{`, `[]`, `null`, `{} {}`, `{"other":1}`, `{"health":{"other":1}}`, `{"timeouts":{"request":123}}`, `{"timeouts":{"request":"tomorrow"}}`} {
		t.Run(raw, func(t *testing.T) {
			if _, err := Load(strings.NewReader(raw)); err == nil {
				t.Fatal("accepted invalid config")
			}
		})
	}
	if _, err := Load(strings.NewReader(strings.Repeat(" ", 1<<20) + `{}`)); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized configuration: %v", err)
	}
}

func TestValidateRejectsInvalidConfiguration(t *testing.T) {
	tests := []struct {
		name   string
		change func(*Config)
		want   string
	}{
		{"remote public", func(c *Config) { c.Listen = "0.0.0.0:8080" }, "loopback"},
		{"remote admin", func(c *Config) { c.AdminListen = "example.com:9090" }, "loopback"},
		{"bad port", func(c *Config) { c.Listen = "127.0.0.1:99999" }, "port"},
		{"same listener", func(c *Config) { c.AdminListen = c.Listen }, "must differ"},
		{"no routes", func(c *Config) { c.Routes = nil }, "routes"},
		{"no upstreams", func(c *Config) { c.Upstreams = nil }, "upstreams"},
		{"duplicate route", func(c *Config) { c.Routes = append(c.Routes, c.Routes[0]) }, "duplicate route"},
		{"duplicate prefix", func(c *Config) { r := c.Routes[0]; r.ID = "two"; c.Routes = append(c.Routes, r) }, "duplicate route prefix"},
		{"duplicate upstream", func(c *Config) { c.Upstreams = append(c.Upstreams, c.Upstreams[0]) }, "duplicate upstream"},
		{"invalid id", func(c *Config) { c.Upstreams[0].ID = "client/controlled" }, "id"},
		{"unknown backend", func(c *Config) { c.Routes[0].Backends = []string{"missing"} }, "unknown backend"},
		{"repeated backend", func(c *Config) { c.Routes[0].Backends = []string{"one", "one"} }, "repeats backend"},
		{"empty group", func(c *Config) { c.Routes[0].Backends = nil }, "no backends"},
		{"bad prefix", func(c *Config) { c.Routes[0].Prefix = "/api/" }, "prefix"},
		{"negative duration", func(c *Config) { c.Health.Timeout = -1 }, "health.timeout"},
		{"no threshold", func(c *Config) { c.Health.SuccessThreshold = 0 }, "threshold"},
		{"no active", func(c *Config) { c.Limits.Active = 0 }, "active"},
		{"negative queue", func(c *Config) { c.Limits.Waiting = -1 }, "waiting"},
		{"body overflow", func(c *Config) { c.Limits.BodyBytes = 1 << 62 }, "body_bytes"},
		{"tiny headers", func(c *Config) { c.Limits.MaxHeaderBytes = 1 }, "max_header_bytes"},
		{"no refill", func(c *Config) { c.Rate.Refill = 0 }, "rate"},
		{"no table", func(c *Config) { c.Rate.MaxEntries = 0 }, "rate"},
		{"unbounded connections", func(c *Config) { c.Timeouts.MaxConnsPerHost = 0 }, "max_conns_per_host"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := valid()
			tt.change(&c)
			err := c.Validate()
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("want diagnostic containing %q, got %v", tt.want, err)
			}
		})
	}
}

func TestUpstreamOriginsAndHealthPaths(t *testing.T) {
	for _, origin := range []string{"file:///tmp/data", "http://", "http://user:password@host", "http://host/api", "http://host/?x=1", "http://host/#secret", "http://host:99999", "http://host:abc", "http://host:", "http://[garbage]", "http://host?", "ftp://host", "http://host/%2F"} {
		t.Run(origin, func(t *testing.T) {
			c := valid()
			c.Upstreams[0].URL = origin
			if err := c.Validate(); err == nil {
				t.Fatal("accepted malformed origin")
			}
		})
	}
	for _, path := range []string{"healthz", "https://remote/healthz", "//remote/healthz", "/healthz?q=1", "/healthz#fragment"} {
		t.Run(path, func(t *testing.T) {
			c := valid()
			c.Upstreams[0].HealthPath = path
			if err := c.Validate(); err == nil {
				t.Fatal("accepted malformed health path")
			}
		})
	}
	for _, origin := range []string{"http://127.0.0.1:8081", "https://example.com", "http://[::1]:8081/"} {
		c := valid()
		c.Upstreams[0].URL = origin
		if err := c.Validate(); err != nil {
			t.Fatal(err)
		}
	}
}
