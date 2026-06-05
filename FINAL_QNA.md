# ShopX AIOps W1 - Final Q&A

## 1. Nguyên nhân gốc rễ là cart-service bị tràn RAM đúng không?

Nói chính xác hơn: root-cause hypothesis là `cart-service` bị heap/memory pressure, likely liên quan `ProductCatalogCache` retention/eviction failure. Metric `memory_usage_bytes` không tự chứng minh vượt limit vì max khoảng 1.70GB trong khi limit là 2.15GB. Bằng chứng vượt limit đến từ log `Container OOMKilled: memory limit exceeded`.

## 2. Evidence mạnh nhất cho memory/heap pressure là gì?

Chuỗi bằng chứng: GC warning lúc `06:30:32` với heap 93%, cache eviction failure lúc `06:33:57`, memory metric vượt MAD threshold lúc `16:26`, GC pause metric vượt threshold lúc `17:50:30`, OutOfMemory/OOMKilled gần `19:59`, rồi restart count tăng lúc `20:00`.

## 3. Kurtosis là gì?

Kurtosis là chỉ số mô tả “đuôi” của phân phối. Kurtosis cao nghĩa là có nhiều extreme values/outliers hơn so với phân phối normal. Trong deck dùng kurtosis để giải thích vì sao mean/std dễ bị ảnh hưởng bởi tail.

## 4. Skewness của data thực sự là bao nhiêu?

Với cart-service: p99 latency skewness `3.31`, restart count `3.20`, 5xx rate `2.77`, memory usage `2.32`, GC pause `1.16`. Đây là lý do nói dataset này right-skewed, không phải claim chung chung cho mọi telemetry.

## 5. Vì sao dùng robust MAD?

Vì nhiều metric có tail phải và outlier. robust MAD dùng median và MAD nên ít bị kéo lệch hơn mean/std. Nó còn cho threshold cụ thể theo từng metric. Tuy nhiên vẫn phải audit false positive theo metric: cart `http_5xx_rate` có raw crossing lúc `06:08`, nhưng baseline false positive quá cao nên bị hạ cấp.

## 6. EWMA bị false positive ở đâu?

EWMA trong source dùng `span=20`, tức alpha `2/(20+1) = 0.095`. Ví dụ `payment-service/upstream_timeout_rate` bị EWMA flag lúc `09:51 UTC`, trong khi MAD downstream symptom rõ là `20:45 UTC`. cart 5xx EWMA cũng có baseline false positives. Vì vậy EWMA không dùng làm detector chính.

## 7. Nếu EWMA không đáng tin thì dùng làm gì?

EWMA dùng để nhìn drift/trend vì nó làm mượt noise. Có thể tuning alpha, nhưng trade-off rất rõ: alpha thấp hơn mượt hơn nhưng trễ hơn; alpha cao hơn nhạy hơn nhưng false positive nhiều hơn. Với dataset này, vấn đề nằm ở metric noisy/traffic ramp, nên tuning alpha không tốt bằng dùng MAD+IF cho decision và EWMA cho visualization.

## 8. IsolationForest có evidence ở đâu?

Có trong `outputs/method_comparison.csv`. IF train trên first 6h, `n_estimators=200`, `contamination=0.03`. Nó flag `cart-service` lúc `07:27 UTC`, top metric là `http_5xx_rate`. Deck mới dùng IF như confirmation, không thay thế MAD.

## 9. Vì sao không dùng IF làm detector chính?

IF là multivariate và khó giải thích threshold theo từng metric. Nó tốt để confirm service abnormality, nhưng báo cáo RCA cần nói rõ metric nào, value bao nhiêu, threshold bao nhiêu, lúc nào. MAD làm việc đó tốt hơn.

## 10. Logs có phù hợp Drain3 không?

Có. Logs là JSONL structured, schema khá consistent, message format lặp lại. Drain3 phù hợp vì message vẫn có dynamic params như userId, orderId, status, duration, heap, pause. Drain3 giúp gom thành template và count theo thời gian.

## 11. Bất thường lúc 06:08 lấy từ đâu, và vì sao bị hạ cấp?

Từ `outputs/anomalies_metrics.csv`: `cart-service/http_5xx_rate` tại `2026-06-01T06:08:00+00:00`, value `1.03`, baseline median `0.065`, MAD threshold `0.354`, score khoảng `10.0σ`. Nhưng audit lại cho thấy chính metric này có `297/720` điểm trong baseline vượt threshold, nên threshold quá nhạy với metric 5xx noisy/zero-inflated. Vì vậy `06:08` chỉ giữ như raw crossing, không dùng làm mốc RCA.

## 11b. Có phải mình tính sai MAD cho 5xx không?

Không. Calculation đúng theo công thức `median + 3 * 1.4826 * MAD`. Sai ở detector choice: metric 5xx noisy/zero-inflated nên MAD threshold `0.354` nằm trong vùng baseline bình thường. Baseline audit cho thấy p75 `1.06`, p95 `2.00`, p99 `2.00`, nên threshold này không qua quality gate.

Detector sửa lại là `http_5xx_sustained`: baseline first 6h, threshold `max(baseline_p99 * 1.5, 3.0) = 3.00`, persistence `5/10` điểm 30s, và volume guard `http_requests_per_sec >= baseline p50`. Kết quả cho cart 5xx: baseline FP `0`, decision `2026-06-01T20:41:30+00:00`, `impact_signal=True`, `supports_rca_chain=False`. Đây là impact evidence, không phải root-cause start.

## 11c. Detector 5xx vượt ngưỡng duy trì là gì?

Nó là rule detector theo cửa sổ thời gian, không phải ML. Thay vì alert ngay khi một điểm 5xx vượt ngưỡng, nó chỉ alert khi lỗi vượt ngưỡng đủ nhiều lần trong một cửa sổ gần nhất.

Trong bản báo cáo, detector chạy như sau:

- Lấy 6 giờ đầu làm baseline.
- Tính p99 baseline của `http_5xx_rate`; với cart-service, p99 baseline là `2.00`.
- Đặt threshold `max(2.00 * 1.5, 3.0) = 3.00`.
- Với mỗi timestamp sau baseline, nhìn lại 10 điểm gần nhất. Vì interval là 30 giây, 10 điểm là 5 phút.
- Alert khi ít nhất 5/10 điểm có `http_5xx_rate > 3.00`.
- Chỉ xét những điểm có `http_requests_per_sec >= baseline p50` để tránh alert từ tỷ lệ lỗi méo mó khi traffic quá thấp.

Cách này giảm false positive rất mạnh: cart 5xx baseline FP từ `297/720` ở MAD xuống `0`.

Kết quả trong `outputs/method_comparison.csv`: `cart-service/http_5xx_rate` được detect lúc `2026-06-01T20:41:30+00:00`. Dòng này được đánh dấu `impact_signal=True` và `supports_rca_chain=False`, nghĩa là nó xác nhận impact 5xx user-facing sau khi cart đã restart/downstream đã có triệu chứng; nó không phải bằng chứng bắt đầu root cause.

Trade-off là detect chậm hơn. Nếu production cần nhạy hơn, nên tách 3 mức: cảnh báo sớm `3/5` với ngưỡng mềm hơn, nghiêm trọng duy trì `5/10` như báo cáo, và bùng nổ tức thì khi 5xx tăng rất lớn. Không nên đổi detector chính thành `1/1` vì sẽ quay lại alert nhiễu.

## 11d. Figure EDA thật support audit 5xx như thế nào?

Deck có hai lớp figure support.

Thứ nhất là figure baseline 6 giờ được generate từ `g1/metrics/cart-service.csv`:

- `outputs/charts/cart-5xx-baseline-6h-audit.png`.
- Bên trái: time series 6 giờ đầu, có MAD threshold `0.354` và các điểm bị flag false positive.
- Bên phải: histogram baseline, có median `0.065`, p75 `1.06`, p95/p99 `2.00`.

Figure này giải thích điểm yếu của baseline: 6 giờ đầu đủ để hiệu chỉnh detector, nhưng riêng metric 5xx đã noisy ngay trong baseline, nên MAD tạo `297/720` false positive.

Thứ hai là output trực tiếp từ `EDA.ipynb`:

- `notebook-figure-01.png`: histogram/density của `cart__http_5xx_rate`.
- `notebook-figure-02.png`: ACF plot của `cart__http_5xx_rate`.

Histogram cho thấy đa số giá trị nằm sát 0 nhưng đuôi kéo dài tới `16.78`; số EDA đi kèm là `p50=0.38`, `p95=9.53`, `p99=14.43`, `skew=2.77`. Vì vậy metric này lệch phải và có spike/tail mạnh.

ACF cho thấy metric có tương quan theo thời gian, nên không nên alert theo từng crossing đơn lẻ. Đây là lý do detector 5xx dùng cửa sổ `5/10` thay vì `1/1`.

## 12. Vậy incident/RCA start nên nói lúc mấy giờ?

Không nên nói `06:08` là reliable RCA start. Mốc reliable evidence đầu tiên là log cart GC warning `06:30:32` và cache eviction failure `06:33:57`. Mốc reliable metric anomaly đầu tiên là cart p99 latency `14:40:00`. OOMKilled gần `19:59:31` là phase visible/nặng hơn, sau đó restart và downstream timeout/5xx lan rộng.

## 13. Alert production nên là gì?

Composite alert cho cart-service: memory slope tăng, GC pause vượt robust baseline, ProductCatalogCache eviction failure template count tăng, p99 latency tăng, 5xx theo 3 mức warning/critical/severe, và guardrail cho OOMKilled + restart loop.

Riêng 5xx nên tách:

- Cảnh báo sớm: ngưỡng mềm hơn, ví dụ `max(p95 * 1.2, 2.0)`, persistence `3/5`.
- Nghiêm trọng duy trì: ngưỡng hiện tại `max(p99 * 1.5, 3.0)`, persistence `5/10`, baseline FP `0`.
- Bùng nổ tức thì: đường tắt khi 5xx bùng nổ, ví dụ `max(p99 * 4.0, 10.0)`, có guard theo traffic.

## 14. Bonus pipeline nên nói gì?

Production version: OpenTelemetry SDK/Collector -> Kafka -> Flink processing -> MAD/IF detector + Drain3 template mining -> VictoriaMetrics/Loki/ClickHouse/S3 -> RCA dashboard. Kafka/Flink cho phép buffer, replay và recalibrate detector.

## 15. Vì sao dùng format ADR cho phần pipeline?

ADR giúp trả lời đủ bốn ý: context là vấn đề gì, quyết định chọn gì, vì sao chọn, và trade-off là gì. Với bài này, ADR-lite làm rõ vì sao chọn OTel, Kafka, Flink, MAD+IF, Drain3 và hot/cold storage thay vì chỉ vẽ architecture cho đẹp.

## 16. Live pipeline hiện tại khác production như thế nào?

Hiện tại repo dùng local replay simulator: đọc CSV/JSONL, phát sự kiện theo timestamp, chạy detector state trong Python, rồi ghi `events.jsonl`, `alerts.jsonl`, `signals.json`, `rca_timeline.json`, `rca_hypotheses.json`. Production sẽ thay file replay bằng OTel ingest, Kafka streaming, Flink window processing và dashboard đọc từ hot stores/RCA service.
