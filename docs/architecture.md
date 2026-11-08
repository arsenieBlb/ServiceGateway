# Architecture

```mermaid
flowchart LR
    Client[HTTP client] --> Server[Loopback HTTP server\nheader/body/idle limits]
    Server --> Policy[Protocol and path routing\npeer token bucket\nhealth availability]
    Policy --> Admission[Active permits and finite wait queue]
    Admission --> Select[Healthy round robin]
    Select --> Body[Read bounded body\nlimit plus detection byte]
    Body --> Proxy[Standard ReverseProxy Rewrite\nshared bounded Transport]
    Proxy --> A[Backend A]
    Proxy --> B[Backend B]
    Proxy --> Client
    Health[Bounded sequential health probes] --> A
    Health --> B
    Health --> State[Thread-safe backend states]
    State --> Policy
    State --> Select
    Admin[Separate loopback admin server] --> Live[/healthz]
    Admin --> Ready[/readyz]
    State --> Ready
    Admin --> Metrics[/metrics JSON]
    Policy --> Metrics
    Admission --> Metrics
    Proxy --> Metrics
```

`cmd/gateway` validates configuration, opens separate traffic and administration listeners, starts health checking and handles termination. `internal/gateway` defines middleware order, ownership of deadlines, body limits, request IDs, errors and shutdown. `internal/proxy` delegates HTTP transport semantics and response streaming to `httputil.ReverseProxy`; it controls the destination authority and forwarded-header policy and observes stream read failures.

`internal/config` is the single static policy schema. `routing` chooses the longest segment-boundary prefix. `upstreams` owns health state and synchronized selection. `health` runs one sequential probe loop per configured backend. `admission` bounds active upstream work and waiting handlers without extra queue-task goroutines. `ratelimit` provides independent token buckets with a bounded peer table and a clock seam. `metrics` records fixed classifications and configured identifiers.

All request ownership remains inside its HTTP handler. A request holds an admission permit while its bounded body is read and its upstream operation/response stream runs. Cancellation, timeout, early return and panic cleanup release it once. Handler lifetime includes the admission queue, so shutdown can join queued and active work. Shutdown closes admissions and exits waiting requests, cancels and joins health loops, lets active work drain until the grace deadline, cancels remaining request contexts, closes idle transport connections and shuts down both servers.

Unknown backend health is intentionally unavailable for routing. Consecutive successful probes make a backend healthy; consecutive failures make it unhealthy. Opposite results reset consecutive counters, while threshold-crossing states persist through intermediate checks. A route's readiness is an OR over its configured backend health; gateway readiness is the AND over all routes.

The demonstration consists of separate executable processes and no external account. `cmd/backend` echoes ordinary API requests and can inject a bounded delay. `cmd/demo` launches the processes, injects dependency loss and overload, measures operations and saves raw evidence plus a generated report. It is a measurement tool, not a production load generator.
