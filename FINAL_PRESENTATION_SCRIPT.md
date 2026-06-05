# ShopX AIOps W1 - HTML PowerPoint Script

Deck mới: `outputs/presentations/shopx-aiops-final-rca-html.pptx`.

## Slide 1 - Answer First

Kết luận chính: origin candidate là `cart-service`. RCA hypothesis không chỉ là downstream timeout, mà là `cart-service` đi vào heap/cache pressure trước khi OOM. Evidence chain gồm GC warning, ProductCatalogCache eviction failure, OOMKilled, restart loop, rồi lỗi lan sang gateway/order/payment.

Về WHEN: mốc log đáng tin đầu tiên là `06:30/06:33 UTC`; mốc metric đáng tin đầu tiên là p99 latency `14:40 UTC`. `06:08` trên cart 5xx chỉ là raw/noisy MAD crossing, không dùng làm incident start.

## Slide 2 - Notebook Data Loaded

Đưa output thật từ `EDA.ipynb`: mỗi service có 2,820 rows, interval 30 giây trong ngày `2026-06-01`, merged shape là `(2820, 27)`. Key metrics có missing count bằng 0. Ý chính: dữ liệu đủ sạch để phân tích, không phải dữ liệu bị thiếu lung tung.

## Slide 3 - EDA Distribution Shape

Dataset này có nhiều metric incident right-skewed. Ví dụ cart p99 latency skew `3.31`, restart `3.20`, 5xx `2.77`, memory `2.32`. Kurtosis cao nghĩa là tail/outlier nặng. Vì vậy robust MAD hợp lý hơn mean/std cho threshold baseline.

## Slide 4 - Notebook Figures Embedded

Các hình histogram/boxplot là output trực tiếp từ notebook. Dùng slide này để chứng minh bằng hình rằng các metric có tail và outlier, không chỉ đọc số trong bảng.

## Slide 5 - Exact MAD Evidence

Reliable MAD evidence map vào số liệu cụ thể:

- `14:40`: p99 latency `148.7ms > 122.8ms`.
- `16:26`: memory `0.62GB > 0.57GB`.
- `17:50:30`: GC pause `131.8ms > 104.3ms`.
- `20:00`: restart count `1 > 0`.

Tất cả lấy từ `outputs/anomalies_metrics.csv`. Caveat quan trọng: cart `http_5xx_rate` có raw MAD crossing lúc `06:08`, nhưng false positive trong baseline là `297/720`, nên không dùng làm mốc RCA.

## Slide 6 - Detector Decision

Final stance: robust MAD + IsolationForest. MAD là primary vì threshold rõ theo metric. IsolationForest confirm cart-service abnormal lúc `07:27`. EWMA chỉ là trend lens vì có false positives, ví dụ payment timeout `09:51` và cart 5xx baseline FP=12.

EWMA trong source dùng `span=20`, tương đương alpha `2/(20+1) = 0.095`. Tuning alpha thấp hơn sẽ mượt hơn nhưng delay hơn; alpha cao hơn nhạy hơn và false positive nhiều hơn. Vì vấn đề chính là baseline/traffic ramp và metric noisy, tuning alpha không biến EWMA thành RCA detector tốt hơn MAD+IF trong dataset này.

## Slide 7 - Metric Evidence

Cart metrics cho thấy pressure tích lũy trước OOM/restart. Memory vượt MAD threshold lúc `16:26`, GC pause vượt threshold lúc `17:50`, restart count tăng lúc `20:00`. Đây là lý do không nên chỉ nhìn OOM gần 20:00.

## Slide 8 - Log Evidence + Drain3

Logs structured và consistent, nên nhận xét “logs khá gọn” là đúng. Drain3 vẫn hữu ích vì message có dynamic params như userId, orderId, status, duration, heap, pause. Drain3 biến chúng thành template đếm được theo thời gian: GC warning, cache eviction failure, OOMKilled, cart 5xx.

## Slide 9 - Evidence Ordering

RCA dựa vào thứ tự bằng chứng:

`06:08` weak 5xx raw crossing, không dùng làm RCA start -> `06:30` GC warning -> `06:33` cache eviction failed -> `14:40` p99 latency threshold -> `16:26` memory threshold -> `17:50` GC threshold -> `19:59` OOMKilled -> `20:00+` fan-out downstream.

Ordering này đặt cart-service trước downstream symptoms.

## Slide 10 - Python Diagrams Asset

This slide keeps the generated diagram, but in simplified form. The key reading order is left to right: ShopX services emit telemetry, OpenTelemetry Collector ingests and enriches it, Kafka buffers and makes it replayable, Flink processes windows, MAD/IF and Drain3 produce evidence, and the dashboard queries hot stores plus replay archive for RCA.

The important point is not every microservice edge; the important point is the production telemetry path from ingest to RCA.

## Slide 11 - Production Live Pipeline

Production scenario: services emit metrics/logs/traces continuously. OTel Collector handles ingest and enrichment. Kafka buffers and makes the stream replayable. Flink computes windows and features. MAD + IF score metric anomalies. Drain3 templates logs. RCA service merges metric/log/trace evidence into an ordered timeline.

Core alert target: memory slope + GC pause + cache eviction template count + cart p99/5xx.

## Slide 12 - Current Repo Live Simulation

Current implementation is a local replay simulator, not a real Kafka/Flink deployment.

Source is `g1/metrics/*.csv` and `g1/logs/*.jsonl`. `w1/lab/realtime.py` replays rows in timestamp order, updates rolling detector state, emits metric/log-template alerts, and writes RCA JSON artifacts.

Production would swap local files for OTel, Kafka, Flink and hot stores, but the RCA logic stays conceptually the same.

## Slide 13 - Simulating The Data Flow

The dashboard demo flow has four stages:

1. Replay: read metrics and log-template events in timestamp order.
2. Gate: apply baseline thresholds, persistence and log-template count gates.
3. Correlate: merge cart memory/GC/cache/OOM signals with downstream timeout/5xx.
4. Present: dashboard shows alert stream, RCA timeline and ranked hypothesis.

Key artifacts: `events.jsonl`, `alerts.jsonl`, `signals.json`, `rca_timeline.json`, `rca_hypotheses.json`.

## Slide 14 - ADR-Style Decisions

Giải thích theo ADR-lite:

Context: nhiều service, telemetry burst, cần RCA replay.
Decision: OTel cho collection, Kafka cho transport, Flink cho stream processing, MAD+IF cho detection, Drain3 cho logs, VM/Loki/S3 cho storage.
Trade-offs: thêm operational complexity, IF kém explainable hơn MAD, nhiều storage cần vận hành.

## Slide 15 - Final Takeaway

Final takeaway: metrics answer WHEN, log templates answer WHERE, evidence ordering supports WHAT. Prevention nên bắt heap/cache pressure trước OOM bằng composite alert: memory slope + GC pause + cache eviction template count + cart latency/5xx.
