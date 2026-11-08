# Local demonstration measurements

Measured 2026-11-07T16:44:37Z using go1.27.3 on windows/amd64, 24 logical CPUs (Intel64 Family 6 Model 198 Stepping 2, GenuineIntel), host `DESKTOP-121E9LO`.

Revision: `5d358fd7a347a64b63b83f4d701673358008bc9c (check source digest; workspace may be uncommitted)`. Source SHA-256 (sorted Go files and go.mod/go.sum, each filename and content NUL-delimited): `4f79c33d95233b2c98ea469c0ff404abc902d6b433d332beb597cb31147cf9f3`.

Command: `C:\Users\bilba\Documents\Projects\ServiceGateway\bin\demo.exe -gateway-bin bin/gateway.exe -backend-bin bin/backend.exe -out demo/results/windows`. Run from the project root. See `raw.json` for exact configuration, every request (including errors), cumulative metrics samples and observed health transitions. Process logs and generated configuration are saved beside this report.

## Method

Three real processes run on ephemeral loopback ports: two independent backend executables and the gateway. The driver hard-stops backend processes to exercise abrupt dependency loss, restarts the same addresses, and polls configured health states before settled failover/recovery phases. Graceful gateway shutdown is verified separately by integration tests; killing the demo processes during cleanup does not establish graceful shutdown.

Direct and gateway baseline phases use the same driver, service handler, host, request path, target rate (50/s), warm-up (300ms), measured duration (1s) and 3 repetitions. They run sequentially, so short measurements remain sensitive to scheduler and machine noise. No performance target is imposed.

All gateway limits remain enabled: active=4, waiting=8, wait=50ms, body=1048576 bytes, per-peer refill=100000/s and burst=100000 (chosen high enough not to bind in this demonstration), request deadline=300ms, response-header timeout=250ms. Slow phase uses 600ms backend delay at 5/s. Overload uses 150ms delay at 1000/s. Response bodies are fully consumed by the driver so completion latency includes them.

Each offered request is one driver operation; completed means an HTTP response was received and read without a transport/body error, including gateway 4xx/5xx responses. Offered/s = offered count / configured phase seconds. Completed/s = completed count / actual elapsed seconds through the last completion. Latency percentiles use nearest rank `ceil(q*N)-1` over all operations, including rejected and failed requests. Status 0 identifies operations without an HTTP response. Raw per-request data allows alternate calculations.

## Results

| Phase | Offered | Completed | Offered/s | Completed/s | p50 ms | p95 ms | p99 ms | Status counts | Backend counts |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | --- | --- |
| warmup-direct-1 | 15 | 15 | 50.00 | 53.53 | 0.000 | 1.032 | 1.032 | 200=15 | a=15 |
| direct-1 | 50 | 50 | 50.00 | 51.01 | 0.000 | 0.000 | 0.524 | 200=50 | a=50 |
| warmup-gateway-1 | 15 | 15 | 50.00 | 53.53 | 0.000 | 1.677 | 1.677 | 200=15 | a=8, b=7 |
| gateway-1 | 50 | 50 | 50.00 | 51.01 | 0.000 | 0.544 | 0.658 | 200=50 | a=25, b=25 |
| warmup-direct-2 | 15 | 15 | 50.00 | 53.54 | 0.000 | 0.778 | 0.778 | 200=15 | a=15 |
| direct-2 | 50 | 50 | 50.00 | 50.98 | 0.000 | 0.000 | 0.000 | 200=50 | a=50 |
| warmup-gateway-2 | 15 | 15 | 50.00 | 53.37 | 0.000 | 0.718 | 0.718 | 200=15 | a=7, b=8 |
| gateway-2 | 50 | 50 | 50.00 | 51.01 | 0.000 | 0.583 | 0.617 | 200=50 | a=25, b=25 |
| warmup-direct-3 | 15 | 15 | 50.00 | 53.49 | 0.000 | 0.520 | 0.520 | 200=15 | a=15 |
| direct-3 | 50 | 50 | 50.00 | 50.97 | 0.000 | 0.513 | 0.537 | 200=50 | a=50 |
| warmup-gateway-3 | 15 | 15 | 50.00 | 53.45 | 0.570 | 1.188 | 1.188 | 200=15 | a=8, b=7 |
| gateway-3 | 50 | 50 | 50.00 | 50.99 | 0.517 | 1.062 | 1.202 | 200=50 | a=25, b=25 |
| backend-a-detection | 15 | 15 | 50.00 | 53.42 | 0.510 | 1.210 | 1.210 | 200=13, 502=2 | b=13 |
| backend-a-down | 50 | 50 | 50.00 | 50.99 | 0.512 | 0.698 | 1.028 | 200=50 | b=50 |
| recovered | 50 | 50 | 50.00 | 50.98 | 0.512 | 0.676 | 1.240 | 200=50 | a=25, b=25 |
| all-down | 50 | 50 | 50.00 | 50.96 | 0.582 | 0.671 | 1.111 | 503=50 | — |
| slow-deadline | 5 | 5 | 5.00 | 4.75 | 251.320 | 251.929 | 251.929 | 504=5 | — |
| overload | 1000 | 1000 | 1000.00 | 946.87 | 0.000 | 50.507 | 198.551 | 200=28, 503=972 | a=14, b=14 |

Gateway cumulative maxima observed in metrics: active **4/4**, waiting **8/8**. The maxima are maintained by admission accounting, not estimated solely from periodic sampling. This bounds proxied upstream work and waiting admission; it does not bound all client connections or process memory.

| Backend transition | Observed delay from process action (ms) |
| --- | ---: |
| a → unhealthy | 106.064 |
| a → healthy | 137.261 |
| a → unhealthy | 63.965 |
| b → unhealthy | 63.965 |
| a → healthy | 123.424 |
| b → healthy | 123.424 |

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
