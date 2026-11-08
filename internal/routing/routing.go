// Package routing matches configured path prefixes without changing request paths.
package routing

import (
	"service-gateway/internal/config"
	"sort"
	"strings"
)

type Router struct{ routes []config.Route }

func New(routes []config.Route) *Router {
	copyRoutes := append([]config.Route(nil), routes...)
	sort.SliceStable(copyRoutes, func(i, j int) bool { return len(copyRoutes[i].Prefix) > len(copyRoutes[j].Prefix) })
	return &Router{copyRoutes}
}

func (r *Router) Match(path string) (config.Route, bool) {
	for _, route := range r.routes {
		if route.Prefix == "/" || path == route.Prefix || strings.HasPrefix(path, route.Prefix+"/") {
			return route, true
		}
	}
	return config.Route{}, false
}
