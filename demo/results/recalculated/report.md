# Local demonstration measurements

Measured 2026-11-07T16:42:38Z using go1.27.3 on linux/amd64, 24 logical CPUs (Intel(R) Core(TM) Ultra 9 275HX), host `9dbbafdb5ac5`.

Revision: `unversioned workspace`. Source SHA-256 (sorted Go files and go.mod/go.sum, each filename and content NUL-delimited): `4f79c33d95233b2c98ea469c0ff404abc902d6b433d332beb597cb31147cf9f3`.

Command: `/root/.cache/go-build/c6/c661ae24299eab1214fe6e55ff2087b04640898339a031b500cf190d414e080e-d/demo -out demo/results/verified`. Run from the project root. See `raw.json` for exact configuration, every request (including errors), cumulative metrics samples and observed health transitions. Process logs and generated configuration are saved beside this report.

## Method

Three real processes run on ephemeral loopback ports: two independent backend executables and the gateway. The driver hard-stops backend processes to exercise abrupt dependency loss, restarts the same addresses, and polls configured health states before settled failover/recovery phases. Graceful gateway shutdown is verified separately by integration tests; killing the demo processes during cleanup does not establish graceful shutdown.

Direct and gateway baseline phases use the same driver, service handler, host, request path, target rate (50/s), warm-up (300ms), measured duration (1s) and 3 repetitions. They run sequentially, so short measurements remain sensitive to scheduler and machine noise. No performance target is imposed.

All gateway limits remain enabled: active=4, waiting=8, wait=50ms, body=1048576 bytes, per-peer refill=100000/s and burst=100000 (chosen high enough not to bind in this demonstration), request deadline=300ms, response-header timeout=250ms. Slow phase uses 600ms backend delay at 5/s. Overload uses 150ms delay at 1000/s. Response bodies are fully consumed by the driver so completion latency includes them.

Each offered request is one driver operation; completed means an HTTP response was received and read without a transport/body error, including gateway 4xx/5xx responses. Offered/s = offered count / configured phase seconds. Completed/s = completed count / actual elapsed seconds through the last completion. Latency percentiles use nearest rank `ceil(q*N)-1` over all operations, including rejected and failed requests. Status 0 identifies operations without an HTTP response. Raw per-request data allows alternate calculations.

## Results

| Phase | Offered | Completed | Offered/s | Completed/s | p50 ms | p95 ms | p99 ms | Status counts | Backend counts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| warmup-direct-1 | 15 | 15 | 50.00 | 53.38 | 0.309 | 4.922 | 4.922 | 200=15 | a=15 |
| direct-1 | 50 | 50 | 50.00 | 50.97 | 0.291 | 0.519 | 0.704 | 200=50 | a=50 |
| warmup-gateway-1 | 15 | 15 | 50.00 | 53.22 | 1.810 | 5.418 | 5.418 | 200=15 | a=8, b=7 |
| gateway-1 | 50 | 50 | 50.00 | 50.93 | 1.479 | 2.941 | 3.709 | 200=50 | a=25, b=25 |
| warmup-direct-2 | 15 | 15 | 50.00 | 53.33 | 0.226 | 0.429 | 0.429 | 200=15 | a=15 |
| direct-2 | 50 | 50 | 50.00 | 50.97 | 0.292 | 0.493 | 0.511 | 200=50 | a=50 |
| warmup-gateway-2 | 15 | 15 | 50.00 | 53.29 | 1.622 | 2.105 | 2.105 | 200=15 | a=7, b=8 |
| gateway-2 | 50 | 50 | 50.00 | 50.92 | 1.479 | 4.289 | 4.898 | 200=50 | a=25, b=25 |
| warmup-direct-3 | 15 | 15 | 50.00 | 53.42 | 0.246 | 0.496 | 0.496 | 200=15 | a=15 |
| direct-3 | 50 | 50 | 50.00 | 50.97 | 0.269 | 0.381 | 0.551 | 200=50 | a=50 |
| warmup-gateway-3 | 15 | 15 | 50.00 | 53.25 | 1.236 | 2.773 | 2.773 | 200=15 | a=8, b=7 |
| gateway-3 | 50 | 50 | 50.00 | 50.93 | 1.340 | 2.515 | 4.011 | 200=50 | a=25, b=25 |
| backend-a-detection | 15 | 15 | 50.00 | 52.73 | 1.126 | 4.610 | 4.610 | 200=13, 502=2 | b=13 |
| backend-a-down | 50 | 50 | 50.00 | 50.91 | 1.416 | 3.993 | 6.060 | 200=50 | b=50 |
| recovered | 50 | 50 | 50.00 | 50.90 | 1.549 | 5.673 | 10.526 | 200=50 | a=25, b=25 |
| all-down | 50 | 50 | 50.00 | 50.86 | 1.345 | 3.846 | 7.600 | 503=50 | — |
| slow-deadline | 5 | 5 | 5.00 | 4.75 | 251.967 | 252.722 | 252.722 | 504=5 | — |
| overload | 1000 | 1000 | 1000.00 | 937.85 | 0.895 | 52.097 | 201.409 | 200=28, 503=972 | a=14, b=14 |

Gateway cumulative maxima observed in metrics: active **4/4**, waiting **8/8**. The maxima are maintained by admission accounting, not estimated solely from periodic sampling. This bounds proxied upstream work and waiting admission; it does not bound all client connections or process memory.

| Backend transition | Observed delay from process action (ms) |
| --- | ---: |
| a → unhealthy | 72.570 |
| a → healthy | 97.285 |
| a → unhealthy | 74.773 |
| b → unhealthy | 75.063 |
| a → healthy | 119.678 |
| b → healthy | 119.925 |

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
