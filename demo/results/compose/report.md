# Local demonstration measurements

Measured 2026-11-07T16:43:04Z using go1.27.3 on linux/amd64, 24 logical CPUs (Intel(R) Core(TM) Ultra 9 275HX), host `57b1fd2491e9`.

Revision: `unversioned workspace`. Source SHA-256 (sorted Go files and go.mod/go.sum, each filename and content NUL-delimited): `4f79c33d95233b2c98ea469c0ff404abc902d6b433d332beb597cb31147cf9f3`.

Command: `/opt/service-gateway/demo -gateway-bin /opt/service-gateway/gateway -backend-bin /opt/service-gateway/backend -out /results`. Run from the project root. See `raw.json` for exact configuration, every request (including errors), cumulative metrics samples and observed health transitions. Process logs and generated configuration are saved beside this report.

## Method

Three real processes run on ephemeral loopback ports: two independent backend executables and the gateway. The driver hard-stops backend processes to exercise abrupt dependency loss, restarts the same addresses, and polls configured health states before settled failover/recovery phases. Graceful gateway shutdown is verified separately by integration tests; killing the demo processes during cleanup does not establish graceful shutdown.

Direct and gateway baseline phases use the same driver, service handler, host, request path, target rate (50/s), warm-up (300ms), measured duration (1s) and 3 repetitions. They run sequentially, so short measurements remain sensitive to scheduler and machine noise. No performance target is imposed.

All gateway limits remain enabled: active=4, waiting=8, wait=50ms, body=1048576 bytes, per-peer refill=100000/s and burst=100000 (chosen high enough not to bind in this demonstration), request deadline=300ms, response-header timeout=250ms. Slow phase uses 600ms backend delay at 5/s. Overload uses 150ms delay at 1000/s. Response bodies are fully consumed by the driver so completion latency includes them.

Each offered request is one driver operation; completed means an HTTP response was received and read without a transport/body error, including gateway 4xx/5xx responses. Offered/s = offered count / configured phase seconds. Completed/s = completed count / actual elapsed seconds through the last completion. Latency percentiles use nearest rank `ceil(q*N)-1` over all operations, including rejected and failed requests. Status 0 identifies operations without an HTTP response. Raw per-request data allows alternate calculations.

## Results

| Phase | Offered | Completed | Offered/s | Completed/s | p50 ms | p95 ms | p99 ms | Status counts | Backend counts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| warmup-direct-1 | 15 | 15 | 50.00 | 53.33 | 0.448 | 1.052 | 1.052 | 200=15 | a=15 |
| direct-1 | 50 | 50 | 50.00 | 50.96 | 0.324 | 0.805 | 0.941 | 200=50 | a=50 |
| warmup-gateway-1 | 15 | 15 | 50.00 | 53.19 | 1.676 | 48.155 | 48.155 | 200=15 | a=8, b=7 |
| gateway-1 | 50 | 50 | 50.00 | 50.88 | 1.786 | 3.763 | 7.504 | 200=50 | a=25, b=25 |
| warmup-direct-2 | 15 | 15 | 50.00 | 53.40 | 0.337 | 0.462 | 0.462 | 200=15 | a=15 |
| direct-2 | 50 | 50 | 50.00 | 51.00 | 0.367 | 0.615 | 0.739 | 200=50 | a=50 |
| warmup-gateway-2 | 15 | 15 | 50.00 | 52.91 | 1.558 | 2.674 | 2.674 | 200=15 | a=7, b=8 |
| gateway-2 | 50 | 50 | 50.00 | 50.96 | 1.406 | 2.829 | 4.068 | 200=50 | a=25, b=25 |
| warmup-direct-3 | 15 | 15 | 50.00 | 53.45 | 0.320 | 1.945 | 1.945 | 200=15 | a=15 |
| direct-3 | 50 | 50 | 50.00 | 50.98 | 0.382 | 0.679 | 1.886 | 200=50 | a=50 |
| warmup-gateway-3 | 15 | 15 | 50.00 | 53.25 | 1.559 | 3.359 | 3.359 | 200=15 | a=8, b=7 |
| gateway-3 | 50 | 50 | 50.00 | 50.90 | 1.608 | 4.129 | 4.539 | 200=50 | a=25, b=25 |
| backend-a-detection | 15 | 15 | 50.00 | 53.20 | 1.751 | 3.498 | 3.498 | 200=14, 502=1 | b=14 |
| backend-a-down | 50 | 50 | 50.00 | 50.90 | 1.646 | 4.063 | 4.552 | 200=50 | b=50 |
| recovered | 50 | 50 | 50.00 | 50.75 | 1.703 | 4.300 | 7.696 | 200=50 | a=25, b=25 |
| all-down | 50 | 50 | 50.00 | 50.89 | 1.448 | 2.060 | 4.615 | 503=50 | — |
| slow-deadline | 5 | 5 | 5.00 | 4.74 | 252.730 | 254.466 | 254.466 | 504=5 | — |
| overload | 1000 | 1000 | 1000.00 | 939.49 | 2.802 | 62.259 | 201.253 | 200=28, 503=972 | a=14, b=14 |

Gateway cumulative maxima observed in metrics: active **4/4**, waiting **8/8**. The maxima are maintained by admission accounting, not estimated solely from periodic sampling. This bounds proxied upstream work and waiting admission; it does not bound all client connections or process memory.

| Backend transition | Observed delay from process action (ms) |
| --- | ---: |
| a → unhealthy | 62.288 |
| a → healthy | 95.560 |
| a → unhealthy | 63.081 |
| b → unhealthy | 63.305 |
| a → healthy | 68.593 |
| b → healthy | 69.374 |

Health times include process scheduling and polling delay; they are measurements of this run rather than universal detection guarantees. Detection-window requests can legitimately fail before failed-backend health transitions.

Checks:

- `active_bound`: true
- `all_down_503`: true
- `failover_only_b`: true
- `overload_rejects`: true
- `recovered_distribution`: true
- `slow_504`: true
- `waiting_bound`: true

These are local demonstration measurements, not production capacity claims. The load driver itself uses one goroutine per offered operation and a bounded maximum offered rate flag; gateway admission bounds apply to gateway work, not driver resources.
