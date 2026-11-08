# service-gateway

[![Checks](https://github.com/arsenieBlb/ServiceGateway/actions/workflows/ci.yml/badge.svg)](https://github.com/arsenieBlb/ServiceGateway/actions/workflows/ci.yml)

[Verification and limitations](docs/application-review.md)

A standalone Go HTTP gateway for ordinary API requests. It selects healthy backends by round robin, streams responses with `net/http/httputil.ReverseProxy`, and bounds upstream work, admission waiting, request bodies and per-client rate-limit state. It has no external Go dependencies or credentials.

The project targets **Go 1.27.3** (`go.mod` records the selected toolchain). Actual local verification and outstanding checks are recorded in [docs/completion.md](docs/completion.md). The GitHub Actions workflow is a definition; it does not imply hosted CI has run.

## Build and check

Install the supported Go toolchain, then run from this directory:

```sh
go version
go vet ./...
go test ./...
go test -race -count=1 ./...
go build -o bin/gateway ./cmd/gateway
go build -o bin/backend ./cmd/backend
```

On Windows, use `bin/gateway.exe` and `bin/backend.exe` for the output filenames. A race build requires a compatible C compiler; the Ubuntu CI job enables CGO and runs the race detector. Local Linux-container race execution, when recorded in completion evidence, is distinct from a hosted CI run.

## Run three independent local processes

In three terminals:

```sh
go run ./cmd/backend -listen 127.0.0.1:9001 -id a
go run ./cmd/backend -listen 127.0.0.1:9002 -id b
go run ./cmd/gateway -config demo/example.json
```

After the configured consecutive health successes, make requests:

```sh
curl -i 'http://127.0.0.1:8080/api/items?limit=2'
curl -i -X POST 'http://127.0.0.1:8080/api/items' -H 'Content-Type: application/json' -d '{"name":"example"}'
curl 'http://127.0.0.1:9090/healthz'
curl 'http://127.0.0.1:9090/readyz'
curl 'http://127.0.0.1:9090/metrics'
```

PowerShell users can use `curl.exe` to avoid older PowerShell's `curl` alias. Stop backend A, watch its health state turn unhealthy in `/metrics`, and repeat the request to see backend B serve it. Restart A on the same address, wait for its healthy transition and observe it rejoin. Stop both backends to receive 503; liveness stays 200 while readiness becomes 503. Ctrl+C stops a foreground process. The gateway stops admissions, cancels health checks and drains admitted work within its configured grace period.

## Repeatable measurements

After building the binaries:

```sh
go run ./cmd/demo -out demo/results/local
```

Windows defaults select the `.exe` binaries. Use `-gateway-bin` and `-backend-bin` to override their locations. The driver launches three real local processes on ephemeral loopback ports, measures direct/backend and gateway baselines with warm-up and three repetitions, hard-stops and recovers backends, measures the detection window, exercises all-backends-down, slow responses and deliberate overload, then stops its processes. It saves every operation, cumulative metrics snapshots, process logs, exact configuration, machine/toolchain/source metadata and a calculated report. It exits unsuccessfully if observed resource bounds or resilience expectations fail.

```sh
go run ./cmd/demo -duration 2s -warmup 500ms -repetitions 3 -rate 50 -overload-rate 1000 -out demo/results/local
go run ./cmd/demo -report-from demo/results/local/raw.json -out demo/results/recalculated
```

The second command recalculates phase counts, rates and nearest-rank percentiles from the saved operations and timing windows, regenerating the report without launching any service. Existing evidence is under `demo/results/` when present. These local observations establish no production throughput guarantee. See each report for limits, denominators, timings, failures and comparison caveats.

Docker is optional. `docker compose run --build --rm demo` builds and runs the same process demonstration inside a Go Linux container, writing results to `demo/results/compose`. It publishes no network ports. Normal Go backends remain independently runnable; Docker is not required to use this project.

## Configuration and operational contract

`demo/example.json` includes every setting. Missing policy fields receive documented defaults visible in `internal/config.Default`; routes and upstreams must be supplied. Unknown JSON fields, malformed durations/URLs, duplicate IDs/prefixes, missing backend references and impossible capacities are rejected. Listeners must bind to loopback. Upstream URLs are configured HTTP/HTTPS origins without credentials, base paths, queries or fragments. HTTPS uses normal system certificate validation; custom development certificates must be trusted correctly.

Routing is longest path-prefix with segment boundaries: `/api` matches `/api` and `/api/items`, and does not match `/apiculture`. Prefix `/` is the catch-all. Paths, encoded paths and query strings are preserved; no rewrite option exists. The outbound Host is the configured upstream authority. Incoming `Forwarded` and every `X-Forwarded-*` assertion are removed; gateway-generated forwarding headers use the socket peer, original host and scheme. Client rate limits always use the socket peer IP, ignoring forwarded assertions.

Each request receives a new 32-character hexadecimal `X-Request-ID`; arbitrary client IDs are replaced. Structured logs contain configured route/backend IDs, status, outcome, duration and request ID. They exclude request/response bodies, raw query strings, authorization and arbitrary headers. Backend HTTP errors pass through and remain separate from gateway transport failures.

Middleware precedence is: process stopping → unsupported CONNECT/upgrade → route matching → peer rate limit → healthy-upstream availability → admission → backend reselection → bounded body read → proxy. Thus a rate-limited request may get 429 even when its route has no healthy backends, and capacity rejection can precede body-size rejection. No waiting request body is buffered. Only admitted bodies are read, at most 1 MiB plus one detection byte by default, then reconstructed before forwarding; responses remain streamed.

| Condition | Response |
| --- | --- |
| Unmatched route | 404 `not_found` |
| CONNECT or protocol upgrade | 501 `unsupported_protocol` |
| Peer bucket depleted | 429 `rate_limited` plus integer seconds `Retry-After` |
| New identity with full limiter table | 429 `limiter_full` plus earliest estimated idle expiry |
| No healthy backend | 503 `no_healthy_upstream` |
| Admission full, waiting timeout or stop | 503 `admission_rejected` or `stopping` |
| Body exceeds configured limit | 413 `body_too_large`, before backend execution |
| Inbound body read timeout/error | 408 `body_timeout` / 400 `body_read` |
| Upstream connection or protocol failure | 502 `connect` |
| Upstream response-header/overall timeout before final response starts | 504 `timeout` |
| Client cancellation | Cancel work and release admission; no new error body |
| Stream failure after headers | Abort stream and count partial failure; preserve already-sent status |

Gateway errors are small JSON objects: `{"error":"code","request_id":"..."}`. Errors rejecting an unread request body close the HTTP/1 connection and expire its read deadline so slow body transmission cannot delay the rejection. HTTP server parsing/header-limit errors can be generated by Go before the gateway handler runs and consequently do not use this schema or gateway accounting. A backend's own 500 is forwarded as a valid response. The overall deadline starts when the handler runs, includes admission/body reading/proxying, and cancels upstream work. Inbound header/body/idle limits, connection/TLS/header deadlines and transport connection limits are separately configured. Streaming writes have a bounded **100ms** allowance beyond the overall deadline so a 504 can be returned before any upstream response; after a stream starts, the status cannot be changed.

The limiter bounds **upstream request work and admission waiting**, not all incoming sockets, all memory or all goroutines. Defaults are 32 active, 64 waiting, 250ms wait, 1 MiB body, 100 tokens/second and burst 200 per socket peer. The client table has 10,000 entries with lazy 5-minute idle expiry. A full table rejects new identities instead of evicting active quotas; rejected existing identities count as activity. Limits are per process. NAT clients share a bucket. Idle cleanup occurs on the next limiter request and adds no background worker.

`/healthz`, `/readyz` and `/metrics` exist only on the administration listener. Health starts unknown/unready; each backend has one sequential bounded probe worker, configurable success/failure thresholds and continued checks after failure. Readiness requires at least one healthy backend in every route. A previously healthy backend can fail before the next check; requests during detection may receive 502/504. The JSON metrics endpoint uses only configured IDs and fixed status/outcome/bucket dimensions: request totals/classes/outcomes/latency buckets, selected-backend counts, transport failures, active/waiting/high-water counts, rate/table rejections and backend states.

There is no application retry loop. Go's shared `http.Transport` may retry certain network errors on a previously successful connection when a request is idempotent and replayable (or has no body), and may retry an unwritten request with no body. The gateway clears `GetBody` and preserves a non-replayable body for unsafe methods, including empty bodies that `ReverseProxy` would otherwise clear. Client idempotency headers remain forwarded but do not make unsafe requests replayable. Safe bodyless methods can retain the transport's own narrow retry behavior. See the [Go transport contract](https://pkg.go.dev/net/http#Transport) and the executable non-replay tests.

This version covers finite ordinary HTTP API operations. WebSockets, CONNECT tunnels, protocol upgrades, SSE as a long-lived protocol, large uploads, distributed limits, dynamic discovery, configuration reload, durable queues and production-scale operation are outside this release. Remote listeners, TLS termination, trusted-proxy address handling and administration authentication need a separate design before external exposure. The loopback admin port is intentionally unauthenticated and must remain local.

Architecture and material choices are in [docs/architecture.md](docs/architecture.md) and [docs/decisions.md](docs/decisions.md). Requirement-by-requirement evidence, executed commands, unresolved checks and proposed CV bullets are in [docs/completion.md](docs/completion.md).
