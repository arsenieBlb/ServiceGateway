# Implementation and verification record

The supplied [implementation plan](implementation-plan.md) is implemented as one independent `service-gateway` project. Everything runs locally without credentials or another repository. No deployment or publication was performed. Material policies and conditional choices are recorded in [decisions.md](decisions.md); setup and exact reproduction commands are in [the README](../README.md).

## Requirement mapping

Paths below are relative to the project root. “Verified” means executed local checks, not a claim of hosted CI or production readiness. There are 48 top-level test functions, with further table cases and subtests. Integration tests use ephemeral loopback ports, channel barriers, observed state with bounded polling, and explicit deadlines. Timed failure injection is asserted against outcomes, rather than relying only on a sleep.

| Mandatory requirement | Implementation | Executed test/evidence | Status |
| --- | --- | --- | --- |
| Independent Go executable and supported, recorded stable toolchain | `go.mod`, `cmd/gateway`, standard library only | Go 1.27.3 Linux build/tests; Windows cross-build; evidence below | Verified |
| Separate testable config/routing/upstreams/health/admission/ratelimit/proxy/metrics modules | `internal/` packages plus gateway composition | Complete package test/build runs | Verified |
| Strict JSON, identifiers, durations, capacities and HTTP/HTTPS origins | `internal/config` defaults, bounded input, validation before listening | `TestLoad*`, `TestValidateRejectsInvalidConfiguration`, `TestUpstreamOriginsAndHealthPaths` | Verified |
| Deterministic longest segment-boundary prefix; preserve original URL | `internal/routing`, `internal/proxy` | `TestLongestBoundaryMatch`, `TestProxyHTTPContract` (escaped path, repeated/query values) | Verified |
| Client cannot choose target host; deliberate outbound Host | Rewrite uses configured upstream scheme/authority; clears URL userinfo | `TestProxyHTTPContract`; config origin tests; implementation inspection | Verified |
| Healthy concurrency-safe round robin | `internal/upstreams` locked health and group cursors | `TestRoundRobinAndReadiness`, `TestConcurrentSelection` (10,000 selections) | Verified |
| Ordinary methods/payloads/status/headers; stream responses | Standard ReverseProxy, bounded request reconstruction | `TestProxyHTTPContract` across GET/POST/PUT/PATCH/DELETE/HEAD/OPTIONS; partial-stream test | Verified |
| Rewrite API; replace forwarded assertions; standard hop-by-hop handling | `internal/proxy` Rewrite and SetXForwarded | `TestProxyHTTPContract` spoofing, request/response connection-nominated headers | Verified |
| Explicit unsupported tunnel/upgrade rejection | CONNECT/requested upgrade 501; backend 101 fails ModifyResponse | `TestProxyErrorsAndOrdering`, `TestReviewEarlyErrorsDoNotDrainIncompleteBodies`; upstream 101 policy inspected | Verified |
| Bounded request IDs, structured secret-free logs and responses | 128-bit random ID; fixed fields and configured IDs | `TestProxyHTTPContract`, `TestAccessLogRedaction` | Verified |
| Unknown/unready initial state; success/failure thresholds | `internal/upstreams` health state machine | `TestThresholdsAndRecovery`, `TestReviewUnknownReadinessAndLiveness` | Verified |
| Bounded probes, at most one per backend; keep probing failed services | `internal/health` serial per-backend loops and check guard, context deadlines, no redirects | `TestNoOverlappingProbeAndCancellation`, `TestProbeDeadline`, `TestProbeBodyDeadlineCountsFailure`, `TestRunContinuesProbingAfterUnhealthy` | Verified |
| Separate loopback health/readiness/metrics listener; readiness AND across routes | `cmd/gateway`, AdminHandler, pool.Ready | config loopback checks; `TestRoundRobinAndReadiness`, `TestReviewUnknownReadinessAndLiveness`; real-process demo | Verified |
| Inbound header/body/idle limits; upstream connect/TLS/header/overall deadlines | Configured HTTP servers, ResponseController and shared bounded transport | `TestReviewInboundHeaderLimits`, `TestReviewInboundBodyTimeouts`, `TestUpstreamRefusalAndDeadlines`; settings inspection | Verified |
| Client cancellation and overall deadline propagate upstream | Request-derived context with gateway cancellation, read/write deadlines | `TestClientDisconnectCancelsUpstream`, `TestLimitsShutdownDeadlineCancelsActiveUpstream`, partial-stream deadline | Verified |
| 404/429/503/413/502/504 consistent error contract | Fixed JSON errors, deterministic middleware ordering | `TestProxyErrorsAndOrdering`, `TestUpstreamRefusalAndDeadlines`, `TestLimitsBodyAndPolicyPrecedence`, raw early-error tests | Verified |
| No new JSON after response starts; abort partial streams | Standard proxy abort panic retained; accounting cleanup observes body errors | `TestPartialStreamAbortsWithoutJSON`, metrics classification tests | Verified |
| Bounded active and separate finite waiting admission | `internal/admission` FIFO waiters in existing request handlers | `TestBoundedActiveAndWaiting`, `TestLimitsCapacityQueuedBodyAndCancellation`; measured maxima in reports | Verified |
| Wait deadline/cancellation, no task goroutine per queue entry, exact permit release | Acquire/Stop/release with mutex and sync.Once | `TestQueueCancellationAndTimeout`, `TestCancelAndGrantRaceDoesNotLeak`, `TestLimitsWaitExpiryAndNoForwarding` | Verified |
| Prompt rejection with incomplete/unread request bodies | Gateway errors close HTTP/1 connection and expire input read deadline | `TestReviewCapacityRejectsIncompleteBodyPromptly`, `TestReviewEarlyErrorsDoNotDrainIncompleteBodies` | Verified |
| Per-peer token buckets, burst/refill, ignore spoofed addresses | `internal/ratelimit`, socket peer parsing | `TestRefillBurstAndClientIsolation`, HTTP contract forwarding tests, policy precedence tests | Verified |
| Bounded limiter table, idle expiry, explicit full policy/counter | Lazy expiry; reject new identities with 429 and estimated earliest idle expiry | `TestIdleExpiryAndFullTablePolicy`, `TestRejectedExistingClientRemainsActive`, concurrent table tests | Verified |
| Meaningful Retry-After; deterministic clock tests | Integer seconds rounded up; injectable Clock | token bucket fake-clock tests, `TestLimitsBodyAndPolicyPrecedence` | Verified |
| Default 1MiB body, read limit+1 only after admission, no declared-length allocation | Bounded LimitReader/ReadAll; reconstruct body; no response buffering | `TestLimitsBodyAndPolicyPrecedence`, `TestLimitsCapacityQueuedBodyAndCancellation`, `TestProxyErrorsAndOrdering`, raw oversized-body test | Verified |
| No application automatic retry, ambiguous POST not duplicated | No retry loop or GetBody; restore nonnil empty unsafe body after Rewrite clone | `TestPOSTIsNotReplayedAfterAmbiguousFailure`, `TestReviewEmptyPOSTIsNotReplayed` with idempotency header | Verified |
| Standard transport retry semantics documented accurately | README and decisions describe idempotent/unwritten retries and unsafe body policy | Official Go API/source inspection and non-replay tests | Verified |
| Graceful shutdown stops admissions/probes, wakes waiters, drains then cancels; joins workers/closes idle connections | Gateway.Shutdown and coordinated server shutdown in executable | `TestLimitsGracefulShutdownDrainsActiveAndStopsQueue`, `TestLimitsShutdownDeadlineCancelsActiveUpstream`, `TestReviewShutdownInterruptsIncompleteBody`; health worker joins | Verified |
| Backend stopping, all-down, recovery; detection-aware assertions | Probe transitions and separate-process demo | Health recovery tests; saved detection/down/recovered/all-down phases | Verified |
| Race-detector verification on supported environment | Linux amd64 Go container with C compiler; CGO race tests | Full `go test -race -count=3 ./...`, `docs/evidence/checks.txt` | Verified |
| Bounded request/status/latency/active/wait/rejection/health/failure metrics | JSON admin endpoint; configured route/backend IDs, fixed classes/outcomes/buckets | `TestLimitsMetricsReconcileAndDimensionsStayBounded`, all `internal/metrics` tests; raw demo snapshots | Verified |
| Backend 500 distinct from gateway failures; partial/canceled accounting | Separate outcome and status-class counts | `TestProxyErrorsAndOrdering`, `TestFixedDimensionsAndFailureClassification` | Verified |
| Repeatable healthy/failed/recovered/slow/overloaded workload | `cmd/backend`, `cmd/demo` actual local processes | Saved raw per-operation results and reports below | Verified |
| Offered/completed rates, statuses, percentiles, resource maxima, distribution and health transitions | Raw operations/windows + cumulative metrics + transition observations | `demo/results/verified/raw.json`, generated report and regenerated equality check | Verified |
| Direct/gateway baseline same environment/workload, warm-up/repetitions, toolchain/machine/revision | Demo metadata, 3 baseline repetitions and source SHA-256 | Saved reports: 50/s offered baseline, 300ms warm-up, 1s windows, all limits enabled | Verified |
| Failures/timeouts included; raw calculations reproduce report; no fabricated throughput target | `cmd/demo/report.go`, `-report-from` | Report regenerated identically; failed detection-window/timeout/overload operations retained | Verified |
| Setup/run/test/demo docs, example config, diagram, contracts and limitations | README, docs, demo/example.json | Executed builds, tests and process/Compose demos | Verified |
| CI workflow with supported race job; distinguish definition from execution | `.github/workflows/ci.yml` Ubuntu/CGO race, vet, builds, demo, artifacts | Workflow inspected; equivalent commands executed locally; hosted CI not run | Definition complete; hosted run unverified |

The stage gates are covered by the rows above: correct proxy path/configuration (1), health/selection (2), limits (3), difficult HTTP/lifecycle (4), instrumentation (5), measured resilience/load (6), and reproducible handoff (7).

## Conditional decisions

| Trigger | Decision/evidence | Status |
| --- | --- | --- |
| Compatible local C compiler/race detector unavailable | Native Windows Go/C toolchain was absent. Docker provides a supported Linux amd64 Go/C toolchain, and full race tests actually ran there. CI retains its own Ubuntu race job. Native Windows race execution was not performed. | Alternative verification completed |
| Exposure beyond local demo | All listener configuration rejects non-loopback hosts. No remote exposure requested. TLS termination/trusted proxy/admin-access design required before a future change. | Not applicable: trigger absent |
| Existing trusted reverse proxy | None used; identity always socket peer, forwarded assertions ignored. | Not applicable: trigger absent |
| Prometheus needed | JSON metrics suffice; no integration requested. | Not applicable: trigger absent |
| Automatic application retries requested | Not requested; no retry loop. Unsafe empty and nonempty POST non-replay covered. | Not applicable: trigger absent |
| WebSockets/SSE/large uploads required | Ordinary finite API scope only; upgrades rejected; SSE/large uploads documented as separate extensions. | Not applicable: trigger absent |
| Client identities exhaust limiter state | Explicit reject-new policy and table-full metric implemented and tested with fake time and concurrent identities. | Implemented and verified |
| Measured buffering/allocation becomes excessive | Responses stream; no excessive allocation was established by these runs. No speculative stack replacement or optimization. Memory profiling was not performed. | Not applicable: trigger absent |
| Docker available | Available; optional Compose real-process demo runs with no published ports and no external networking. Direct Go commands remain available. | Implemented and executed |
| Local TLS upstream included | Demo uses HTTP. A TLS integration test explicitly trusts the generated test certificate using a root CA pool; no verification bypass. | Implemented and verified for test |

## Executed checks and evidence

Verification date: **2026-11-07** (Europe/Paris). Windows workspace; Docker Desktop Linux execution; Go **1.27.3 linux/amd64**. The container image is `golang:1.27.3-bookworm`, retrieved digest `sha256:dc0963f32053e4bc451d24d87e3892d42d01021b61f093e98eb95c0352bb84b0`. Machine details and exact source identity are also captured in each measurement report. The project is an unversioned workspace inside an unrelated parent Git tree; the saved source digest identifies tested code without attributing it to the parent's revision.

The final full verification command executed from PowerShell (normal, race, analysis and builds all succeed only if preceding checks succeed):

```powershell
docker run --rm -v 'C:\Users\bilba\Documents\Projects\ServiceGateway:/workspace' -v service-gateway-go-cache:/root/.cache/go-build -w /workspace golang:1.27.3-bookworm sh -c 'go version && go test -count=1 ./... && go test -race -count=3 ./... && go vet ./... && go build ./... && GOOS=windows GOARCH=amd64 go build -o bin/gateway.exe ./cmd/gateway && GOOS=windows GOARCH=amd64 go build -o bin/backend.exe ./cmd/backend && GOOS=windows GOARCH=amd64 go build -o bin/demo.exe ./cmd/demo'
```

Output is saved in [evidence/checks.txt](evidence/checks.txt). Earlier targeted component, regression and race checks also passed. The initial review checks deliberately failed on two genuine defects (slow unread-body error delivery and empty POST replay); both defects were fixed and the same regression tests pass in the final run. Builds and vet are silent on success.

Linux final demonstration and exact calculation reproduction:

```sh
go build -o bin/gateway ./cmd/gateway
go build -o bin/backend ./cmd/backend
go run ./cmd/demo -out demo/results/verified
go run ./cmd/demo -report-from demo/results/verified/raw.json -out demo/results/recalculated
```

These commands were run inside the same Go container with the workspace mounted. The regeneration is compared byte-for-byte with the saved report. A second regeneration executed the Windows `bin/demo.exe` against Linux raw data and also matched identically; hashes are saved in [evidence/report-reproduction.txt](evidence/report-reproduction.txt). The [verified report](../demo/results/verified/report.md) and [raw observations](../demo/results/verified/raw.json) contain all results, calculations, transient errors and metadata. `backend_requests` in metrics counts selections, including a selected backend whose body is subsequently rejected; report backend distribution comes from actual backend responses.

Optional convenience check actually executed: `docker compose run --build --rm demo`; evidence is [the Compose report](../demo/results/compose/report.md). An initial Compose attempt could not allocate a Docker network because this machine's pools were exhausted. Running the loopback-only demo with `network_mode: none` resolved it without changing the host's network configuration.

The cross-built Windows binaries were also **executed successfully on the Windows host**:

```powershell
.\bin\demo.exe -gateway-bin bin/gateway.exe -backend-bin bin/backend.exe -out demo/results/windows
```

See [the Windows report](../demo/results/windows/report.md), [raw observations](../demo/results/windows/raw.json) and [command output](evidence/windows-demo.txt). Source digest for all three final demonstrations is `4f79c33d95233b2c98ea469c0ff404abc902d6b433d332beb597cb31147cf9f3`. The Windows report records an enclosing Git revision with an explicit uncommitted-source caveat; that unrelated parent revision is not this project's version, and the matching digest is the relevant identifier.

Each final Linux, Compose and Windows run recorded **1,560 operations**, all seven resilience checks passed, and the measured active/waiting maxima were **4/4 and 8/8**. In each overload phase, 1,000 offered operations produced **28 HTTP 200 and 972 controlled HTTP 503 responses**. Settled failover sent all 50 operations to B; recovery distributed 25/25; all-down produced 50 HTTP 503 responses; the slow phase produced five HTTP 504 responses. The Linux detection window also retained two expected HTTP 502 responses before failure detection. Initial Linux unhealthy/recovery observations were 72.570ms/97.285ms; Windows observations were 106.064ms/137.261ms. These describe these short local runs, not general capacity or universal transition times.

Linux direct baseline p95 latencies across three repetitions were 0.519/0.493/0.381ms, versus gateway p95 2.941/4.289/2.515ms at the same offered 50 operations/second. Full distributions and actual completion denominators remain in the reports. Some Windows submillisecond observations resolve to zero; this is recorded measurement granularity, not a claim of zero latency. No optimization or production-scale throughput claim is inferred.

## Exact demonstration steps

1. Install Go 1.27.3 and open a terminal in this project, or use the Docker Compose command above without installing Go on the host.
2. Run `go build -o bin/gateway ./cmd/gateway` and `go build -o bin/backend ./cmd/backend` (append `.exe` to output filenames on Windows).
3. Run `go run ./cmd/demo -out demo/results/local`. It starts separate gateway/A/B processes on ephemeral loopback ports and checks healthy traffic, backend loss/detection, settled failover, recovery, all-down, slow deadlines and overload.
4. Open `demo/results/local/report.md`; inspect `raw.json` for every operation and metrics sample. Regenerate with `go run ./cmd/demo -report-from demo/results/local/raw.json -out demo/results/recalculated`.
5. For interactive use, start the two `go run ./cmd/backend` commands and `go run ./cmd/gateway -config demo/example.json` in separate terminals as shown in the README; query ports 8080/9090, stop/restart a backend, then Ctrl+C the gateway to drain it.

## Remaining verification boundaries

Hosted GitHub Actions has not executed: only its workflow definition and equivalent local Linux checks exist. Native Windows race detection and native Go-source builds were not performed because host Go/C tools are absent; Linux race verification is complete and Windows binaries were cross-built. These local checks do not establish production capacity, long-duration leak behavior, TLS termination, remote administration security or unsupported protocol behavior. No cloud deployment, publishing, API credentials or other portfolio project is needed or was used.

## Proposed CV bullets — one Service Gateway project entry

- Built an independent Go HTTP gateway with health-aware round robin, streamed reverse proxying, request deadlines, bounded admission, per-client token buckets, structured logs and JSON metrics; verified cancellation, graceful draining and ambiguous POST non-replay with repeated race-detector tests.
- Created a repeatable real-process resilience/load harness with reproducible reports; recorded 1,560 operations per Linux/Windows run, maintained 4 active/8 waiting limits, and returned 972 controlled 503s during a 1,000-request overload while verifying backend recovery and 504 deadlines.
