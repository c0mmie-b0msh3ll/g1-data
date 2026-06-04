# ShopX AIOps W1 - HTML PowerPoint Script

Deck mới: `outputs/presentations/shopx-aiops-final-rca-html.pptx`.

## Slide 1 - Answer First

Kết luận chính: origin candidate là `cart-service`. RCA hypothesis không chỉ là downstream timeout, mà là `cart-service` đi vào heap/cache pressure trước khi OOM. Evidence chain gồm GC warning, ProductCatalogCache eviction failure, OOMKilled, restart loop, rồi lỗi lan sang gateway/order/payment.

## Slide 2 - Notebook Data Loaded

Đưa output thật từ `EDA.ipynb`: mỗi service có 2,820 rows, interval 30 giây trong ngày `2026-06-01`, merged shape là `(2820, 27)`. Key metrics có missing count bằng 0. Ý chính: dữ liệu đủ sạch để phân tích, không phải dữ liệu bị thiếu lung tung.

## Slide 3 - EDA Distribution Shape

Dataset này có nhiều metric incident right-skewed. Ví dụ cart p99 latency skew `3.31`, restart `3.20`, 5xx `2.77`, memory `2.32`. Kurtosis cao nghĩa là tail/outlier nặng. Vì vậy robust MAD hợp lý hơn mean/std cho threshold baseline.

## Slide 4 - Notebook Figures Embedded

Các hình histogram/boxplot là output trực tiếp từ notebook. Dùng slide này để chứng minh bằng hình rằng các metric có tail và outlier, không chỉ đọc số trong bảng.

## Slide 5 - Exact MAD Evidence

Timestamp anomaly map vào số liệu cụ thể:

- `06:08`: cart 5xx `1.03 > 0.354`.
- `14:40`: p99 latency `148.7ms > 122.8ms`.
- `16:26`: memory `0.62GB > 0.57GB`.
- `17:50:30`: GC pause `131.8ms > 104.3ms`.

Tất cả lấy từ `outputs/anomalies_metrics.csv`.

## Slide 6 - Detector Decision

Final stance: robust MAD + IsolationForest. MAD là primary vì threshold rõ theo metric. IsolationForest confirm cart-service abnormal lúc `07:27`. EWMA chỉ là trend lens vì có false positives, ví dụ payment timeout `09:51` và cart 5xx baseline FP=12.

## Slide 7 - Metric Evidence

Cart metrics cho thấy pressure tích lũy trước OOM/restart. Memory vượt MAD threshold lúc `16:26`, GC pause vượt threshold lúc `17:50`, restart count tăng lúc `20:00`. Đây là lý do không nên chỉ nhìn OOM gần 20:00.

## Slide 8 - Log Evidence + Drain3

Logs structured và consistent, nên nhận xét “logs khá gọn” là đúng. Drain3 vẫn hữu ích vì message có dynamic params như userId, orderId, status, duration, heap, pause. Drain3 biến chúng thành template đếm được theo thời gian: GC warning, cache eviction failure, OOMKilled, cart 5xx.

## Slide 9 - Evidence Ordering

RCA dựa vào thứ tự bằng chứng:

`06:08` metric 5xx -> `06:30` GC warning -> `06:33` cache eviction failed -> `16:26` memory threshold -> `17:50` GC threshold -> `19:59` OOMKilled -> `20:00+` fan-out downstream.

Ordering này đặt cart-service trước downstream symptoms.

## Slide 10 - Production Pipeline Diagram

Diagram được generate bằng Python `diagrams` package. Pipeline production: services -> OTel SDK/Collector -> Kafka topics -> Flink processing -> MAD/IF + Drain3 -> VictoriaMetrics/Loki/ClickHouse/S3 -> RCA dashboard.

Lý do chính: Kafka buffer/replay, Flink rolling features, storage tách hot/cold, dashboard trả lời WHEN/WHERE/WHAT.

## Slide 11 - ADR-Style Decisions

Giải thích theo ADR-lite:

Context: nhiều service, telemetry burst, cần RCA replay.
Decision: OTel cho collection, Kafka cho transport, Flink cho stream processing, MAD+IF cho detection, Drain3 cho logs, VM/Loki/S3 cho storage.
Trade-offs: thêm operational complexity, IF kém explainable hơn MAD, nhiều storage cần vận hành.

## Slide 12 - Final Takeaway

Final takeaway: metrics answer WHEN, log templates answer WHERE, evidence ordering supports WHAT. Prevention nên bắt heap/cache pressure trước OOM bằng composite alert: memory slope + GC pause + cache eviction template count + cart latency/5xx.
