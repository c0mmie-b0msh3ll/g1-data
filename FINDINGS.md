# FINDINGS

## Executive Summary

WHEN: the earliest sustained metric anomaly begins at `2026-06-01T06:08:00+00:00`. The silent cart-service signals are GC/cache/memory pressure before the user-facing alert burst.

WHERE: the primary origin is `cart-service`, led by memory pressure, JVM GC pauses, and cache eviction failure logs. The downstream symptoms appear later in `order-service`, `payment-service`, and `api-gateway`.

WHAT: the evidence supports a cart-service memory pressure incident. ProductCatalogCache eviction failures and rising GC pauses preceded `OOMKilled`; OOMKilled then drove pod restarts and connection refusals/timeouts, which propagated as cart 5xx and upstream timeout rates in callers.

## Evidence Timeline

| timestamp                        | stage              | service         | signal_type   | signal                   | evidence                                                    | rca_interpretation                                      |
|:---------------------------------|:-------------------|:----------------|:--------------|:-------------------------|:------------------------------------------------------------|:--------------------------------------------------------|
| 2026-06-01T06:08:00+00:00        | metric anomaly     | cart-service    | metric        | http_5xx_rate            | robust MAD anomaly begins at 2026-06-01T06:08:00+00:00      | cart degradation starts before downstream alert fan-out |
| 2026-06-01T06:30:32.992000+00:00 | log signal         | cart-service    | log_template  | GC warning               | GC overhead limit warning: pause=713ms heap=93%             | log evidence supports metric ordering                   |
| 2026-06-01T06:33:57.795000+00:00 | log signal         | cart-service    | log_template  | cache eviction failure   | ProductCatalogCache eviction failed: heap pressure too high | log evidence supports metric ordering                   |
| 2026-06-01T14:40:00+00:00        | metric anomaly     | cart-service    | metric        | http_p99_latency_ms      | robust MAD anomaly begins at 2026-06-01T14:40:00+00:00      | cart degradation starts before downstream alert fan-out |
| 2026-06-01T16:26:00+00:00        | metric anomaly     | cart-service    | metric        | memory_usage_bytes       | robust MAD anomaly begins at 2026-06-01T16:26:00+00:00      | cart degradation starts before downstream alert fan-out |
| 2026-06-01T17:50:30+00:00        | metric anomaly     | cart-service    | metric        | jvm_gc_pause_ms_avg      | robust MAD anomaly begins at 2026-06-01T17:50:30+00:00      | cart degradation starts before downstream alert fan-out |
| 2026-06-01T19:59:26.256000+00:00 | log signal         | cart-service    | log_template  | connection refused       | Upstream connection refused host=product-service            | log evidence supports metric ordering                   |
| 2026-06-01T19:59:31.047000+00:00 | log signal         | cart-service    | log_template  | OOMKilled                | Container OOMKilled: memory limit exceeded                  | log evidence supports metric ordering                   |
| 2026-06-01T20:00:00+00:00        | metric anomaly     | cart-service    | metric        | container_restart_count  | robust MAD anomaly begins at 2026-06-01T20:00:00+00:00      | cart degradation starts before downstream alert fan-out |
| 2026-06-01T20:00:00+00:00        | restart loop       | cart-service    | metric        | container_restart_count  | restart counter first increases to 1                        | OOM/restart cycle becomes externally visible            |
| 2026-06-01T20:00:58.858000+00:00 | log signal         | order-service   | log_template  | order timeout            | Cart service timeout after 2467ms                           | log evidence supports metric ordering                   |
| 2026-06-01T20:08:00+00:00        | downstream symptom | api-gateway     | metric        | cart_upstream_error_rate | robust MAD anomaly begins at 2026-06-01T20:08:00+00:00      | cart failures propagate to callers                      |
| 2026-06-01T20:30:03.634000+00:00 | log signal         | order-service   | log_template  | order sees cart 5xx      | Cart service returned 5xx status=500                        | log evidence supports metric ordering                   |
| 2026-06-01T20:32:00+00:00        | downstream symptom | order-service   | metric        | upstream_timeout_rate    | robust MAD anomaly begins at 2026-06-01T20:32:00+00:00      | cart failures propagate to callers                      |
| 2026-06-01T20:45:00+00:00        | downstream symptom | payment-service | metric        | upstream_timeout_rate    | robust MAD anomaly begins at 2026-06-01T20:45:00+00:00      | cart failures propagate to callers                      |

## Method Choice

The final primary detector is robust 3-alpha/MAD against the first 6 hours because it is explainable and produces service/metric evidence that maps directly to the incident. IsolationForest is retained as a multivariate confirmation method. EWMA is used for trend smoothing and early slope visualization, not as the main classifier.

For readers who learned the classic 3-alpha rule as `mean + 3 * std`: this report uses the same 3-alpha idea, but with robust statistics. Instead of `mean`, it uses the baseline `median`. Instead of `std`, it uses `1.4826 * MAD`, where `MAD = median(|x - median(x)|)`. The factor `1.4826` converts MAD to a standard-deviation-like scale when the data is approximately normal. This makes the threshold less sensitive to baseline spikes:

```text
classic 3-alpha: mean + 3 * std
robust 3-alpha: median + 3 * 1.4826 * MAD
```

## Log Calibration

Selected Drain3 config: `sim_th=0.6`, `depth=4`, `max_children=100`, `parametrize_numeric_tokens=True`.

Reason: the calibration table shows low similarity settings coarsen related failures, while `sim_th=0.6` keeps GC warning, cache eviction failure, OOMKilled, cart timeout, and cart 5xx patterns distinct.

## Key Template Evidence

| template_id   | service       | level   |   count | first_seen                       | template                                                    |
|:--------------|:--------------|:--------|--------:|:---------------------------------|:------------------------------------------------------------|
| T001          | cart-service  | INFO    |    3186 | 2026-06-01T00:00:19.437000+00:00 | Health check passed                                         |
| T002          | cart-service  | INFO    |    3158 | 2026-06-01T00:00:18.233000+00:00 | Item added to cart for userId=<NUM>                         |
| T003          | cart-service  | INFO    |    2822 | 2026-06-01T00:00:19.656000+00:00 | DB query executed table=cart <*>                            |
| T004          | cart-service  | WARN    |    2671 | 2026-06-01T06:33:57.795000+00:00 | ProductCatalogCache eviction failed: heap pressure too high |
| T005          | order-service | INFO    |    2391 | 2026-06-01T00:00:01.817000+00:00 | Cart service call succeeded                                 |
| T006          | order-service | INFO    |    2238 | 2026-06-01T00:00:16.169000+00:00 | Order created orderId=<ORDER_ID> userId=<NUM>               |
| T007          | cart-service  | WARN    |    2084 | 2026-06-01T06:30:32.992000+00:00 | GC overhead limit warning: pause=<NUM>ms heap=<NUM>%        |
| T008          | cart-service  | INFO    |    2041 | 2026-06-01T00:02:10.733000+00:00 | Checkout completed orderId=<ORDER_ID>                       |
| T009          | cart-service  | INFO    |    1871 | 2026-06-01T00:02:10.942000+00:00 | ProductCatalogCache loaded <*> entries                      |
| T010          | order-service | INFO    |    1845 | 2026-06-01T00:00:39.809000+00:00 | Health check passed                                         |
| T011          | cart-service  | WARN    |    1552 | 2026-06-01T06:34:56.284000+00:00 | Slow response detected endpoint=/api/cart <*>               |
| T012          | cart-service  | WARN    |    1104 | 2026-06-01T00:04:45.868000+00:00 | Connection pool nearing limit pool=db <*>                   |
