package routing

import (
	"service-gateway/internal/config"
	"testing"
)

func TestLongestBoundaryMatch(t *testing.T) {
	r := New([]config.Route{{ID: "api", Prefix: "/api"}, {ID: "users", Prefix: "/api/users"}})
	for _, tt := range []struct {
		path, id string
		ok       bool
	}{
		{"/api", "api", true}, {"/api/", "api", true}, {"/api/items", "api", true}, {"/api/users", "users", true}, {"/api/users/123", "users", true}, {"/api/users2", "api", true}, {"/apiculture", "", false}, {"/", "", false}, {"/API", "", false},
	} {
		t.Run(tt.path, func(t *testing.T) {
			route, ok := r.Match(tt.path)
			if ok != tt.ok || route.ID != tt.id {
				t.Fatalf("got %s %v", route.ID, ok)
			}
		})
	}
	r = New([]config.Route{{ID: "all", Prefix: "/"}, {ID: "api", Prefix: "/api"}})
	if route, _ := r.Match("/api/one"); route.ID != "api" {
		t.Fatal("root shadowed specific route")
	}
	if route, _ := r.Match("/anything"); route.ID != "all" {
		t.Fatal("missing catchall")
	}
}
