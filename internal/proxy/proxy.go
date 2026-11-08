// Package proxy delegates HTTP semantics and streaming to the standard proxy.
package proxy

import (
	"io"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
)

type observedBody struct {
	io.ReadCloser
	failed func(error)
}

func (b observedBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil && err != io.EOF {
		b.failed(err)
	}
	return n, err
}

func New(target *url.URL, transport http.RoundTripper, requestID string, onError func(http.ResponseWriter, *http.Request, error), onReadError func(error)) *httputil.ReverseProxy {
	return &httputil.ReverseProxy{
		Transport: transport,
		// The parent emits bounded structured errors without raw URLs or bodies.
		ErrorLog: log.New(io.Discard, "", 0),
		Rewrite: func(p *httputil.ProxyRequest) {
			p.Out.URL.User = nil
			p.Out.URL.Scheme = target.Scheme
			p.Out.URL.Host = target.Host
			p.Out.URL.Path = p.In.URL.Path
			p.Out.URL.RawPath = p.In.URL.RawPath
			p.Out.URL.RawQuery = p.In.URL.RawQuery
			p.Out.Host = target.Host
			for key := range p.Out.Header {
				if strings.EqualFold(key, "Forwarded") || strings.HasPrefix(strings.ToLower(key), "x-forwarded-") {
					p.Out.Header.Del(key)
				}
			}
			p.SetXForwarded()
			p.Out.Header.Set("X-Request-ID", requestID)
			p.Out.GetBody = nil
			// ReverseProxy clears empty bodies to enable Transport retries. Keep
			// unsafe methods non-replayable even with a client idempotency header.
			switch p.Out.Method {
			case http.MethodGet, http.MethodHead, http.MethodOptions, http.MethodTrace:
			default:
				if p.Out.Body == nil {
					p.Out.Body = p.In.Body
				}
			}
		},
		ModifyResponse: func(r *http.Response) error {
			if r.StatusCode == http.StatusSwitchingProtocols {
				return errUpgrade{}
			}
			r.Header.Set("X-Request-ID", requestID)
			r.Body = observedBody{r.Body, onReadError}
			return nil
		},
		ErrorHandler: onError,
	}
}

type errUpgrade struct{}

func (errUpgrade) Error() string { return "upstream upgrade unsupported" }
