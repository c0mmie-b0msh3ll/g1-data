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

Vì nhiều metric có tail phải và outlier. robust MAD dùng median và MAD nên ít bị kéo lệch hơn mean/std. Nó còn cho threshold cụ thể theo từng metric, ví dụ cart 5xx tại `06:08` có value `1.03` vượt threshold `0.354`.

## 6. EWMA bị false positive ở đâu?

Ví dụ `payment-service/upstream_timeout_rate` bị EWMA flag lúc `09:51 UTC`, trong khi MAD downstream symptom rõ là `20:45 UTC`. cart 5xx EWMA cũng có baseline false positives. Vì vậy EWMA không dùng làm detector chính.

## 7. Nếu EWMA không đáng tin thì dùng làm gì?

EWMA dùng để nhìn drift/trend vì nó làm mượt noise. Nó hữu ích cho visualization và early trend review, nhưng không nên dùng một mình để kết luận RCA trong dataset này.

## 8. IsolationForest có evidence ở đâu?

Có trong `outputs/method_comparison.csv`. IF train trên first 6h, `n_estimators=200`, `contamination=0.03`. Nó flag `cart-service` lúc `07:27 UTC`, top metric là `http_5xx_rate`. Deck mới dùng IF như confirmation, không thay thế MAD.

## 9. Vì sao không dùng IF làm detector chính?

IF là multivariate và khó giải thích threshold theo từng metric. Nó tốt để confirm service abnormality, nhưng báo cáo RCA cần nói rõ metric nào, value bao nhiêu, threshold bao nhiêu, lúc nào. MAD làm việc đó tốt hơn.

## 10. Logs có phù hợp Drain3 không?

Có. Logs là JSONL structured, schema khá consistent, message format lặp lại. Drain3 phù hợp vì message vẫn có dynamic params như userId, orderId, status, duration, heap, pause. Drain3 giúp gom thành template và count theo thời gian.

## 11. Bất thường lúc 06:08 lấy từ đâu?

Từ `outputs/anomalies_metrics.csv`: `cart-service/http_5xx_rate` tại `2026-06-01T06:08:00+00:00`, value `1.03`, baseline median `0.065`, MAD threshold `0.354`, score khoảng `10.0σ`.

## 12. Vì sao vẫn nói 06:08 dù OOMKilled gần 20:00?

06:08 là earliest sustained metric anomaly theo robust MAD. OOMKilled gần `19:59:31` là phase visible/nặng hơn. RCA cần phân biệt lúc degradation bắt đầu với lúc container bị kill.

## 13. Alert production nên là gì?

Composite alert cho cart-service: memory slope tăng, GC pause vượt robust baseline, ProductCatalogCache eviction failure template count tăng, cart 5xx/p99 latency tăng, và guardrail cho OOMKilled + restart loop.

## 14. Bonus pipeline nên nói gì?

Production version: OpenTelemetry SDK/Collector -> Kafka -> Flink processing -> MAD/IF detector + Drain3 template mining -> VictoriaMetrics/Loki/ClickHouse/S3 -> RCA dashboard. Kafka/Flink cho phép buffer, replay và recalibrate detector.

## 15. Vì sao dùng format ADR cho phần pipeline?

ADR giúp trả lời đủ bốn ý: context là vấn đề gì, quyết định chọn gì, vì sao chọn, và trade-off là gì. Với bài này, ADR-lite làm rõ vì sao chọn OTel, Kafka, Flink, MAD+IF, Drain3 và hot/cold storage thay vì chỉ vẽ architecture cho đẹp.
