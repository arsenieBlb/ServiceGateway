# Service Gateway AI implementation plan

Build a standalone HTTP gateway in Go that routes requests to backend services and behaves predictably when those services fail or traffic exceeds capacity. The result should demonstrate networking, concurrency, resource management and operational reliability through an independent working tool.

Give this entire document to a coding AI in its own project workspace. It defines required features, conditional choices and completion checks. The AI should implement and verify the gateway, rather than return another plan.

## Instructions for the implementing AI

Create an independent project named `service-gateway`. Read the workspace and applicable instructions before editing and preserve existing work. Its build, demonstration and tests must work without FrameGuard, ModelsController, Log Investigator or DurableKV.

Use a supported stable Go toolchain, recording the actual tested version in the project configuration/documentation. Prefer `net/http`, `net/http/httputil`, contexts, the standard testing package and standard JSON configuration. Use the existing reverse-proxy implementation for HTTP protocol handling; implement routing, health policy, admission control, rate limiting and instrumentation yourself. Keep external dependencies few and justified.

Implement required stages in order. Resolve routine choices and record material decisions in `docs/decisions.md`. CONDITIONAL items become required only when their condition applies. Complete independent work while clearly identifying unavailable checks. Do not automatically deploy or publish the project.

Maintain `docs/completion.md`, mapping each mandatory requirement and conditional decision to implementation, tests, evidence and status. Mark conditional items not applicable only when their trigger is absent, with a reason. An unavailable mandatory check remains unverified.

## Product and supported scope

A client calls the gateway; the gateway chooses a configured healthy backend, forwards the request and returns its response. When dependencies fail, clients receive consistent errors within bounded time. When capacity is exhausted, the gateway rejects excess work predictably.

Version one supports ordinary HTTP API requests with bounded request bodies and streamed responses. It runs as one gateway process with static configuration and a few local demonstration backends. Bind listeners to loopback by default.

The core release does not require Kubernetes, service discovery, a web dashboard, distributed rate limits, a custom HTTP parser, WebSockets, CONNECT tunnels, caching or a service mesh. Document unsupported protocol upgrades and reject them clearly.

## Architecture and configuration

Use modules equivalent to `config`, `routing`, `upstreams`, `health`, `admission`, `ratelimit`, `proxy` and `metrics`. Keep these components testable without external services. A layout such as `cmd/gateway/`, `internal/`, `demo/`, `tests/` and `docs/` is sufficient.

Use a validated JSON configuration file with named routes and upstreams. Reject unknown fields, duplicate route/backend identifiers, invalid durations, impossible capacities and malformed upstream URLs. Only configured HTTP/HTTPS upstreams may receive traffic; the incoming request must never choose an arbitrary target host.

Define deterministic path-prefix routing, including boundary behaviour: a route for `/api` matches `/api` and `/api/...`, but not `/apiculture`. Choose the longest matching prefix. Preserve the original path and query by default and document any optional rewriting separately.

Provide `/healthz`, `/readyz` and `/metrics` on a separate loopback administration listener. Liveness means the process is running. Readiness requires at least one healthy upstream for every configured route group. Backend failures may affect readiness without making the process itself dead.

## Required traffic and failure behaviour

### Routing and proxy correctness

REQUIRED: Select healthy backends using round robin with concurrency-safe state. Stream response bodies using the standard reverse proxy; do not read entire responses into memory. Preserve normal methods, query strings, payloads and response headers/statuses according to HTTP semantics.

Use the supported `ReverseProxy.Rewrite` API where available in the chosen stable toolchain. Deliberately set the outbound URL and Host behaviour. Remove incoming `Forwarded` and `X-Forwarded-*` assertions before creating the gateway's own forwarded information. Let the standard library handle hop-by-hop headers; test the observable behaviour rather than manually rebuilding HTTP.

Generate a bounded request identifier and include it in gateway logs and responses. Avoid logging secrets or request/response bodies by default. Keep configured IDs, rather than arbitrary URLs or client values, as metric dimensions.

### Health state

REQUIRED: Probe each upstream's configured health endpoint with bounded time and at most one active probe per upstream. Begin in an explicit unknown/unready state. Use configurable success/failure thresholds to transition healthy/unhealthy and avoid flapping.

Continue probing unhealthy services so they can rejoin after recovery. Prevent state races between checks, routing and shutdown. An upstream may fail between a successful check and a request; return a clear request error rather than promising zero failures during detection.

### Timeouts and cancellation

REQUIRED: Configure inbound header/body/idle limits and upstream connection, response-header and overall request deadlines. Reuse a shared transport with bounded connection settings. Propagate client cancellation and gateway deadlines to upstream work.

Classify errors consistently: no healthy upstream or rejected capacity returns 503; upstream connection/protocol errors return 502; upstream deadline expiration returns 504 when a response has not started. If headers/body have already been sent, terminate the stream and record the error rather than append an unrelated JSON error or claim the status was changed.

### Admission and bounded waiting

REQUIRED: Set a maximum number of active proxied requests and a separate finite number of waiting requests. Waiting must have a deadline and respect cancellation. Release every permit exactly once on success, error, cancellation and shutdown. Do not create a goroutine per queued task on top of each request handler.

Initial configurable demonstration defaults can be 32 active requests, 64 waiting requests and a 250 ms maximum wait. These are design defaults, not performance claims. State the exact resource boundary: this limits upstream request work and waiting admission; it does not establish a universal bound on all memory or incoming network connections.

### Rate limiting and body limits

REQUIRED: Implement a token bucket per client with configurable refill and burst. Identify clients from the socket peer address by default. Ignore spoofed forwarded addresses. Bound the number of limiter entries and expire idle entries; a full table must have an explicit rejection/eviction policy that cannot grow without limit.

Return 429 with a meaningful Retry-After value when rate limited. Use a clock abstraction to test refill and expiry deterministically. A per-process limit is sufficient; do not describe it as a global distributed limit.

For version one's small API payloads, enforce a default maximum body size of 1 MiB. After admission, read at most that limit plus the detection byte before forwarding, so an oversized request can be rejected with 413 without partially executing it upstream. Reconstruct the bounded request body accurately. Do not buffer waiting requests' bodies or allocate from untrusted declared lengths. Response bodies remain streamed.

### Retries and shutdown

REQUIRED: Do not implement application-level automatic retries in version one. Document the selected Go transport's own retry semantics. In particular, never add a retry loop that replays a state-changing POST after an ambiguous failure.

Handle normal termination by stopping new admissions, cancelling health loops and draining admitted requests within a configured grace period. Waiting requests must exit promptly; requests remaining after the deadline must be cancelled. Close idle upstream connections and finish without leaked background workers.

## Initial observable contract

| Situation | Required outcome |
| --- | --- |
| Unmatched path | 404 with a small consistent error response. |
| Unsupported tunnel or protocol upgrade | A documented explicit rejection; no accidental open proxy/tunnel. |
| Healthy route | Correct method/path/query/body forwarded to one selected backend. |
| All matching upstreams unhealthy | 503 promptly, with no unbounded waiting for recovery. |
| Client over its configured rate | 429 and Retry-After. |
| Active and waiting capacities exhausted | 503 promptly. |
| Waiting deadline expires | 503; remove the request from waiting state. |
| Oversized body | 413 before upstream forwarding. |
| Upstream connection failure | 502 unless another documented status condition applies. |
| Upstream deadline before response begins | 504. |
| Client disconnect | Cancel associated work and release resources; do not attempt to deliver a new error body. |
| Failure during a response stream | Abort correctly and record a partial-response failure. |

Define and test middleware ordering so conflicting conditions have predictable precedence. Distinguish gateway-generated errors from valid error responses returned by the backend; a backend's 500 is not automatically a gateway transport failure.

## Implementation stages

### Stage 1 Create one correct proxy path

Create the Go module, configuration, executable, one route and two tiny local backend services. Implement path selection and standard reverse proxying with explicit host/header behaviour. Use ephemeral ports in tests.

Gate: integration tests verify methods, encoded paths, query strings, headers, payloads and backend status/body propagation. Configuration failures have useful diagnostics.

### Stage 2 Implement health and backend selection

Add the health state machine, bounded probes, round robin and readiness semantics. Expose test seams for time/probe results. Demonstrate one backend stopping and later rejoining.

Gate: tests verify unknown/healthy/unhealthy transitions, thresholds, failure of all backends and recovery. Failover assertions wait for the documented health transition rather than assume immediate detection.

### Stage 3 Implement limits and failure control

Add deadlines, cancellation, admission limits, bounded waiting, token buckets, limiter-entry limits and request-body enforcement. Define error response schemas and request IDs.

Gate: concurrent tests prove active/waiting counts never exceed configured limits, cancelled requests release capacity, client limits remain isolated and oversized requests do not reach a backend. No test relies only on a manual screenshot or a sleep.

### Stage 4 Exercise difficult HTTP behaviour

Test client cancellation, backend connection refusal, slow headers, slow responses, partial streams, spoofed forwarding headers and non-replay of POST requests. Add graceful shutdown tests with both queued and active work. Test graceful draining separately from abrupt process termination.

Gate: downstream/upstream contexts finish, resource counters settle and lifecycle tests detect leaked gateway-owned workers. Use Go's race detector on a supported environment.

### Stage 5 Add useful instrumentation

Expose bounded metrics for request counts/status classes, latency buckets, active/waiting requests, admission/rate-limit rejections, health state and transport failures. A documented JSON endpoint is sufficient initially. Keep access logs structured and redact sensitive fields.

Gate: metric totals reconcile with controlled test traffic, failed/partial requests are classified correctly and metric labels cannot grow from arbitrary paths or request IDs.

### Stage 6 Run repeatable resilience and load demonstrations

Provide scripts or a small Go load driver that run a healthy workload, stop a backend, recover it, introduce slow responses and deliberately exceed configured capacity. Provide a repeatable setup using local Go processes; Docker Compose is an optional convenience.

Capture offered and completed request rates, status counts, latency percentiles, active/waiting maxima, backend distribution and health transition times. For latency baselines, compare direct-backend and gateway requests using the same workload/environment and document which limits are enabled. Include warm-up, duration, repetitions, toolchain, machine and revision.

Do not set an invented requests-per-second target as a completion criterion. Correctness and observed configured bounds are mandatory; throughput results must come from measurements.

Gate: raw results and calculations reproduce the report, including failures and timeout periods.

### Stage 7 Package and hand off

Provide setup/run/test/demo commands, example configuration, an architecture diagram, error/timeout contracts, operational limitations and a CI workflow. Keep at least one CI race-detector job on a supported platform. Distinguish a workflow definition from actual executed CI results.

Gate: a developer can reproduce the gateway demonstration without credentials, a cloud account or another portfolio repository.

## Conditional implementation decisions

| Condition | Required response |
| --- | --- |
| A compatible C toolchain/race detector is unavailable locally | Run normal tests locally and define a supported CI race job. Mark race verification pending until it actually runs; do not report it as passed. |
| The gateway will be exposed beyond local demonstration | Add an explicit TLS/trusted-proxy/admin-access design before enabling remote listeners. Keep the default local configuration unchanged. |
| It must run behind an existing trusted proxy | Add a configured allowlist of trusted proxy networks and precise client-address parsing; test spoofing and untrusted peers. |
| Prometheus integration is needed | Add an exporter using the same bounded metric dimensions after core accounting is correct. A dashboard is optional. |
| Automatic retries are later requested | Specify eligible methods, replayability, attempt limits and total deadline first. Test ambiguous failures and prove state-changing requests are not accidentally duplicated. Keep version one behaviour available. |
| Actual requirements include WebSockets, SSE or large uploads | Treat them as a separately tested protocol/streaming extension with adjusted deadlines and resource bounds; do not claim support through the ordinary API demo. |
| Many client identities exhaust the limiter table | Apply the documented bounded policy and expose a counter. Never silently remove the bound. |
| Measured response buffering or allocation becomes excessive | Profile and optimise the responsible component, retaining cancellation and bounds tests. Do not replace the HTTP stack speculatively. |
| Docker is available | Add a Compose convenience demo if useful, while retaining independently runnable Go backends. |
| Local TLS upstreams are included | Configure proper trust material for tests. Do not disable certificate verification as a default shortcut. |

DEFERRED: Kubernetes, dynamic service discovery, distributed rate limiting, background configuration reloads, custom TLS/HTTP implementations, an administration UI, persistent request queues, durable message delivery and claims of production-scale operation.

## Final acceptance checklist

- [ ] Independent Go executable and deterministic configuration.
- [ ] Correct route matching and proxy semantics.
- [ ] Concurrency-safe health state and backend selection.
- [ ] Bounded active requests, bounded waiting and bounded limiter state.
- [ ] Enforced deadlines, body limits and cancellation.
- [ ] Defined status/error contract and safe handling of partial streams.
- [ ] No application retry loop replaying state-changing requests.
- [ ] Tested graceful shutdown and recovery of backends.
- [ ] Race verification completed or precisely identified as pending.
- [ ] Reproducible failure/load demonstration with raw measurements.
- [ ] Documentation, CI definition and actual verification evidence.

The final AI handoff must list features, conditional decisions, commands actually executed, outcomes, unresolved checks and exact demonstration steps. Include two proposed CV bullets grounded in completed code and saved measurements. Keep the gateway's features under a single project entry.

## Official references

Use these to check API behaviour and supported toolchains during implementation. The gateway policies above are the proposed design.

- [Go reverse proxy](https://pkg.go.dev/net/http/httputil#ReverseProxy) documents supported proxy hooks and behaviour.
- [Go HTTP server and transport](https://pkg.go.dev/net/http) documents deadlines, shutdown, transport reuse and protocol semantics.
- [Go race detector](https://go.dev/doc/articles/race_detector) documents use and supported execution requirements.
- [Cloudflare software engineering internship](https://job-boards.greenhouse.io/cloudflare/jobs/8245197) provides the role connection to network services, infrastructure and Go. The particular gateway design is a portfolio proposal.
