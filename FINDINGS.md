# FINDINGS

## Executive Summary

WHEN: bằng chứng RCA đáng tin đầu tiên bắt đầu từ log GC/cache của `cart-service` tại `2026-06-01T06:30:32.992000+00:00` / `2026-06-01T06:33:57.795000+00:00`. Metric anomaly đáng tin đầu tiên là `cart-service/http_p99_latency_ms` tại `2026-06-01T14:40:00+00:00`. Raw MAD crossing sớm hơn của `cart-service/http_5xx_rate` tại `2026-06-01T06:08:00+00:00` chỉ được giữ như detector failure case, vì baseline false positive quá cao.

WHERE: origin candidate chính là `cart-service`, được hỗ trợ bởi chuỗi memory pressure, JVM GC pause, cache eviction failure, OOMKilled và restart loop. `order-service`, `payment-service`, và `api-gateway` là downstream symptoms xuất hiện muộn hơn.

WHAT: bằng chứng ủng hộ hypothesis `cart-service` bị heap/cache pressure. ProductCatalogCache eviction failure và GC pause tăng trước `OOMKilled`; OOMKilled kéo theo pod restart, connection refused/timeout, rồi lan thành cart 5xx và upstream timeout ở caller.

## Evidence Timeline

| timestamp                        | stage              | service         | signal_type   | signal                   | evidence                                                                                  | rca_interpretation                                                               |
|:---------------------------------|:-------------------|:----------------|:--------------|:-------------------------|:------------------------------------------------------------------------------------------|:---------------------------------------------------------------------------------|
| 2026-06-01T06:30:32.992000+00:00 | log signal         | cart-service    | log_template  | GC warning               | GC overhead limit warning: pause=713ms heap=93%                                           | log evidence supports metric ordering                                            |
| 2026-06-01T06:33:57.795000+00:00 | log signal         | cart-service    | log_template  | cache eviction failure   | ProductCatalogCache eviction failed: heap pressure too high                               | log evidence supports metric ordering                                            |
| 2026-06-01T14:40:00+00:00        | metric anomaly     | cart-service    | metric        | http_p99_latency_ms      | robust MAD anomaly begins at 2026-06-01T14:40:00+00:00                                    | cart degradation starts before downstream alert fan-out                          |
| 2026-06-01T16:26:00+00:00        | metric anomaly     | cart-service    | metric        | memory_usage_bytes       | robust MAD anomaly begins at 2026-06-01T16:26:00+00:00                                    | cart degradation starts before downstream alert fan-out                          |
| 2026-06-01T17:50:30+00:00        | metric anomaly     | cart-service    | metric        | jvm_gc_pause_ms_avg      | robust MAD anomaly begins at 2026-06-01T17:50:30+00:00                                    | cart degradation starts before downstream alert fan-out                          |
| 2026-06-01T19:59:26.256000+00:00 | log signal         | cart-service    | log_template  | connection refused       | Upstream connection refused host=product-service                                          | log evidence supports metric ordering                                            |
| 2026-06-01T19:59:31.047000+00:00 | log signal         | cart-service    | log_template  | OOMKilled                | Container OOMKilled: memory limit exceeded                                                | log evidence supports metric ordering                                            |
| 2026-06-01T20:00:00+00:00        | metric anomaly     | cart-service    | metric        | container_restart_count  | robust MAD anomaly begins at 2026-06-01T20:00:00+00:00                                    | cart degradation starts before downstream alert fan-out                          |
| 2026-06-01T20:00:00+00:00        | restart loop       | cart-service    | metric        | container_restart_count  | restart counter first increases to 1                                                      | OOM/restart cycle becomes externally visible                                     |
| 2026-06-01T20:00:58.858000+00:00 | log signal         | order-service   | log_template  | order timeout            | Cart service timeout after 2467ms                                                         | log evidence supports metric ordering                                            |
| 2026-06-01T20:08:00+00:00        | downstream symptom | api-gateway     | metric        | cart_upstream_error_rate | robust MAD anomaly begins at 2026-06-01T20:08:00+00:00                                    | cart failures propagate to callers                                               |
| 2026-06-01T20:30:03.634000+00:00 | log signal         | order-service   | log_template  | order sees cart 5xx      | Cart service returned 5xx status=500                                                      | log evidence supports metric ordering                                            |
| 2026-06-01T20:32:00+00:00        | downstream symptom | order-service   | metric        | upstream_timeout_rate    | robust MAD anomaly begins at 2026-06-01T20:32:00+00:00                                    | cart failures propagate to callers                                               |
| 2026-06-01T20:41:30+00:00        | impact signal      | cart-service    | metric        | sustained 5xx impact     | http_5xx_sustained decision: 5 of 10 points above threshold 3.0 with traffic volume guard | user-facing impact evidence after restart/downstream symptoms; not the RCA start |
| 2026-06-01T20:45:00+00:00        | downstream symptom | payment-service | metric        | upstream_timeout_rate    | robust MAD anomaly begins at 2026-06-01T20:45:00+00:00                                    | cart failures propagate to callers                                               |

## Method Choice

Detector chính cho RCA vẫn là robust 3-alpha/MAD trên 6 giờ baseline đầu vì dễ giải thích: mỗi anomaly map trực tiếp về service, metric, value, median, threshold và score. IsolationForest chỉ giữ vai trò xác nhận đa biến. EWMA dùng để nhìn drift/trend, không dùng làm classifier chính.

Exception quan trọng là `cart-service/http_5xx_rate`. MAD calculation không sai, nhưng detector choice sai cho metric 5xx vì metric này quá noisy/zero-inflated. Với baseline 6 giờ đầu:

- baseline median: `0.065`
- MAD threshold: `0.354`
- baseline p75: `1.06`
- baseline p95/p99: `2.00`
- baseline false positive: `297/720`

Nói cách khác, ngưỡng MAD `0.354` nằm trong vùng nhiễu baseline bình thường, nên raw crossing lúc `06:08` không được dùng làm RCA start.

Detector sửa lại cho 5xx là `http_5xx_sustained`, tức detector vượt ngưỡng duy trì theo cửa sổ thời gian. Đây là rule detector, không phải ML model. Mục tiêu của nó là tránh alert theo từng điểm đơn lẻ trên một metric error-rate vốn đã noisy trong baseline.

- baseline: 6 giờ đầu, tương đương 720 điểm ở interval 30 giây
- baseline p99 của cart 5xx: `2.00`
- threshold: `max(baseline_p99 * 1.5, 3.0) = 3.00`
- persistence: cần `5/10` điểm gần nhất vượt ngưỡng; với interval 30 giây, window 10 điểm tương đương 5 phút
- volume guard: chỉ xét khi `http_requests_per_sec >= baseline p50`, để tránh tỷ lệ lỗi bị phóng đại khi traffic quá thấp
- kết quả: detect `2026-06-01T20:41:30+00:00`
- baseline FP: `0`
- classification: `impact_signal=True`, `supports_rca_chain=False`

Kết quả này là impact evidence cho user-facing 5xx sau restart/downstream symptoms, không phải root-cause start. Nói ngắn gọn: MAD trả lời “một điểm có vượt ngưỡng robust không?”, còn `http_5xx_sustained` trả lời “5xx có vượt ngưỡng đủ lâu, trên traffic đủ lớn, để coi là impact thật không?”.

Với công thức 3-alpha cổ điển `mean + 3 * std`, báo cáo này dùng cùng trực giác 3-alpha nhưng thay bằng thống kê robust. Thay vì `mean`, dùng baseline `median`. Thay vì `std`, dùng `1.4826 * MAD`, với `MAD = median(|x - median(x)|)`. Hệ số `1.4826` đưa MAD về scale gần standard deviation khi dữ liệu gần normal:

```text
classic 3-alpha: mean + 3 * std
robust 3-alpha: median + 3 * 1.4826 * MAD
```

## EDA Figure Support For 5xx Audit

Deck mới dùng thêm figure baseline 6 giờ được generate trực tiếp từ `g1/metrics/cart-service.csv`:

- `outputs/charts/cart-5xx-baseline-6h-audit.png`: time series + histogram baseline 6 giờ đầu của `cart-service/http_5xx_rate`.

Lý do dùng baseline 6 giờ: đây là cửa sổ trước giai đoạn OOM/restart, đủ 720 điểm ở interval 30 giây để tính median, MAD và percentile. Tuy nhiên chính figure baseline cho thấy điểm yếu của metric 5xx: MAD threshold `0.354` nằm quá thấp so với nhiễu baseline, khiến `297/720` điểm baseline bị flag.

Slide audit cũng dùng figure thật từ `EDA.ipynb`:

- `outputs/presentations/notebook-assets/notebook-figure-01.png`: histogram/density của `cart__http_5xx_rate`.
- `outputs/presentations/notebook-assets/notebook-figure-02.png`: ACF plot của `cart__http_5xx_rate`.

Histogram cho thấy phần lớn giá trị nằm sát 0 nhưng có đuôi kéo dài đến `16.78`; output EDA ghi `skew=2.77`, `p50=0.38`, `p95=9.53`, `p99=14.43`, `max=16.78`. Đây là bằng chứng trực quan rằng metric lệch phải và có spike/tail mạnh.

ACF plot cho thấy metric có tương quan theo thời gian, nên không nên đọc từng crossing đơn lẻ. Vì vậy detector 5xx cần đọc theo cửa sổ thời gian: số điểm vượt ngưỡng trong một window, cộng với guard theo traffic.

## Detector Sensitivity Trade-off

Trade-off của sustained detector là: ít false positive hơn nhưng detect chậm hơn. Bản báo cáo giữ mức `critical duy trì` vì mục tiêu là chứng minh impact chắc chắn với baseline FP `0`.

Nếu đưa vào production, nên tách 3 mức thay vì biến detector chính thành `1/1`:

- Cảnh báo sớm: threshold `max(p95 * 1.2, 2.0)`, persistence `3/5`, guard theo traffic. Mục tiêu là cảnh báo sớm, chấp nhận nhiễu hơn.
- Nghiêm trọng duy trì: threshold `max(p99 * 1.5, 3.0)`, persistence `5/10`, guard theo traffic. Đây là mức dùng trong báo cáo.
- Bùng nổ tức thì: threshold `max(p99 * 4.0, 10.0)`, persistence `1/1` hoặc `2/3`, guard traffic thấp hơn. Mục tiêu là bắt ngay khi 5xx bùng nổ rất lớn.

Cách nói ngắn khi trình bày: “Cảnh báo sớm khi có dấu hiệu nghi ngờ, alert nghiêm túc khi lỗi duy trì, và page ngay khi 5xx bùng nổ.”

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
