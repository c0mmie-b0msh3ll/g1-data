# ShopX AIOps W1 - HTML PowerPoint Script

Deck hiện tại cho vòng này: `outputs/presentations/shopx-aiops-final-html.html`. Không dùng `.pptx` stale cho bản chỉnh sửa này.

## Slide 1 - Trả Lời Trước

Kết luận chính: origin candidate là `cart-service`. RCA hypothesis không chỉ là downstream timeout, mà là `cart-service` đi vào heap/cache pressure trước khi OOM. Chuỗi bằng chứng gồm GC warning, ProductCatalogCache eviction failure, OOMKilled, restart loop, rồi lỗi lan sang gateway/order/payment.

Về WHEN: mốc log đáng tin đầu tiên là `06:30/06:33 UTC`; mốc metric đáng tin đầu tiên là p99 latency `14:40 UTC`. `06:08` trên cart 5xx chỉ là raw/noisy MAD crossing, không dùng làm incident start.

## Slide 2 - Dữ Liệu Đã Load

Đưa output thật từ `EDA.ipynb`: mỗi service có 2,820 dòng, interval 30 giây trong ngày `2026-06-01`, bảng merge có shape `(2820, 27)`. Các metric chính có missing count bằng 0. Ý chính: dữ liệu đủ sạch để phân tích.

## Slide 3 - Hình Dạng Phân Phối EDA

Dataset này có nhiều metric lệch phải. Ví dụ cart p99 latency skew `3.31`, restart `3.20`, 5xx `2.77`, memory `2.32`. Kurtosis cao nghĩa là tail/outlier nặng. Vì vậy robust MAD hợp lý hơn mean/std cho threshold baseline.

## Slide 4 - Hình Từ Notebook

Các hình histogram/boxplot là output trực tiếp từ notebook. Dùng slide này để chứng minh bằng hình rằng các metric có tail và outlier, không chỉ đọc số trong bảng.

## Slide 5 - EDA Baseline 6 Giờ Cho 5xx

Slide này giải thích cửa sổ baseline mình dùng cho detector. Baseline là 6 giờ đầu của ngày `2026-06-01`, tức 720 điểm với interval 30 giây. Lý do dùng 6 giờ: đây là cửa sổ trước giai đoạn OOM/restart, đủ dài để tính median, MAD và percentile.

Figure trên slide được generate lại từ `g1/metrics/cart-service.csv`. Bên trái là time series 6 giờ đầu của `http_5xx_rate`, với đường MAD threshold `0.354` và các điểm false positive. Bên phải là histogram baseline, có median `0.065`, p75 `1.06`, p95/p99 `2.00`.

Điểm cần lưu ý: chính baseline đã có `297/720` điểm vượt MAD threshold. Vì vậy lỗi không nằm ở công thức MAD, mà ở việc dùng MAD một điểm cho metric 5xx quá nhiễu.

## Slide 6 - Bằng Chứng MAD Chính Xác

Reliable MAD evidence map vào số liệu cụ thể:

- `14:40`: p99 latency `148.7ms > 122.8ms`.
- `16:26`: memory `0.62GB > 0.57GB`.
- `17:50:30`: GC pause `131.8ms > 104.3ms`.
- `20:00`: restart count `1 > 0`.

Tất cả lấy từ `outputs/anomalies_metrics.csv`. Caveat quan trọng: cart `http_5xx_rate` có raw MAD crossing lúc `06:08`, threshold `0.354`, nhưng false positive trong baseline là `297/720`, nên đây là detector failure case, không dùng làm mốc RCA.

## Slide 7 - Audit Detector 5xx

Slide này trả lời thẳng câu “có mishandle gì không?”: calculation MAD đúng, nhưng chọn detector MAD cho cart 5xx là sai cho đến khi audit. Baseline của 5xx có median `0.065`, p75 `1.06`, p95/p99 `2.00`; MAD threshold `0.354` nằm trong vùng baseline nhiễu nên tạo FP `297/720`.

Detector sửa lại là `http_5xx_sustained`. Đây không phải ML model; nó là rule theo cửa sổ thời gian cho metric error-rate. Cách tính cụ thể: lấy baseline 6 giờ đầu, tính p99 của `http_5xx_rate`, đặt ngưỡng `max(baseline_p99 * 1.5, 3.0) = 3.00`; sau đó chỉ alert nếu trong 10 điểm gần nhất có ít nhất 5 điểm vượt ngưỡng. Vì dữ liệu có interval 30 giây, cửa sổ này tương đương 5 phút gần nhất và yêu cầu lỗi lặp lại trong khoảng đó.

Detector còn có volume guard: chỉ xét khi `http_requests_per_sec >= baseline p50`. Guard này tránh trường hợp traffic quá thấp làm một vài lỗi đơn lẻ tạo tỷ lệ 5xx nhìn rất lớn nhưng không đại diện cho impact thật.

Kết quả thực tế trong `outputs/method_comparison.csv`: MAD raw crossing của `cart-service/http_5xx_rate` là `06:08:00 UTC` với baseline FP `297/720`; detector `http_5xx_sustained` phát hiện `cart-service/http_5xx_rate` lúc `20:41:30 UTC`, baseline FP `0`, `impact_signal=True`, `supports_rca_chain=False`. Nghĩa là detector này dùng để xác nhận 5xx ảnh hưởng người dùng sau restart/downstream symptoms, không dùng làm root-cause start.

EWMA vẫn chỉ là trend lens. Với 5xx noisy/zero-inflated, tuning EWMA không thay thế được percentile threshold + persistence + volume guard.

## Slide 8 - Figure EDA Cho Audit MAD

Slide này dùng figure thật từ `EDA.ipynb` cho `cart__http_5xx_rate`. Histogram cho thấy phần lớn giá trị nằm sát 0 nhưng có đuôi kéo tới `16.78`, nên ngưỡng MAD `0.354` quá thấp so với vùng nhiễu baseline. ACF cho thấy metric có tương quan theo thời gian, vì vậy không nên đọc từng crossing đơn lẻ.

Ý chính: audit không chỉ dựa vào cảm giác. Figure EDA thật giải thích vì sao 5xx cần đọc theo cửa sổ thời gian.

## Slide 9 - Tuning Độ Nhạy 5xx

Trade-off của sustained detector là ít false positive hơn nhưng detect chậm hơn. Bản báo cáo giữ mức `critical duy trì`: ngưỡng `max(p99 * 1.5, 3.0)`, persistence `5/10`, baseline FP `0`.

Nếu đưa vào production, nên tách 3 mức:

- Cảnh báo sớm: `3/5`, ngưỡng mềm hơn `max(p95 * 1.2, 2.0)`.
- Nghiêm trọng duy trì: `5/10`, ngưỡng hiện tại `max(p99 * 1.5, 3.0)`.
- Bùng nổ tức thì: đường tắt khi 5xx bùng nổ, ví dụ `max(p99 * 4.0, 10.0)` với guard theo traffic.

Không nên đổi thẳng thành `1/1` cho detector chính vì sẽ quay lại vấn đề alert nhiễu.

## Slide 10 - Bằng Chứng Metric

Cart metrics cho thấy pressure tích lũy trước OOM/restart. Memory vượt MAD threshold lúc `16:26`, GC pause vượt threshold lúc `17:50`, restart count tăng lúc `20:00`. Đây là lý do không nên chỉ nhìn OOM gần 20:00.

## Slide 11 - Bằng Chứng Log + Drain3

Logs structured và consistent, nên nhận xét “logs khá gọn” là đúng. Drain3 vẫn hữu ích vì message có dynamic params như userId, orderId, status, duration, heap, pause. Drain3 biến chúng thành template đếm được theo thời gian: GC warning, cache eviction failure, OOMKilled, cart 5xx.

## Slide 12 - Evidence Ordering

RCA dựa vào thứ tự bằng chứng:

`06:08` MAD false early alert/noisy 5xx crossing -> `06:30` GC warning -> `06:33` cache eviction failed -> `14:40` p99 latency threshold -> `16:26` memory threshold -> `17:50` GC threshold -> `19:59` OOMKilled -> `20:00+` fan-out downstream -> `20:41:30` sustained 5xx impact detector.

Ordering này đặt cart-service trước downstream symptoms.

## Slide 13 - Python Diagrams Asset

This slide keeps the generated diagram, but in simplified form. The key reading order is left to right: ShopX services emit telemetry, OpenTelemetry Collector ingests and enriches it, Kafka buffers and makes it replayable, Flink processes windows, MAD/IF and Drain3 produce evidence, and the dashboard queries hot stores plus replay archive for RCA.

The important point is not every microservice edge; the important point is the production telemetry path from ingest to RCA.

## Slide 14 - Production Live Pipeline

Production scenario: services emit metrics/logs/traces continuously. OTel Collector handles ingest and enrichment. Kafka buffers and makes the stream replayable. Flink computes windows and features. MAD + IF score metric anomalies. Drain3 templates logs. RCA service merges metric/log/trace evidence into an ordered timeline.

Core alert target: memory slope + GC pause + cache eviction template count + cart p99/5xx.

## Slide 15 - Current Repo Live Simulation

Current implementation is a local replay simulator, not a real Kafka/Flink deployment.

Source is `g1/metrics/*.csv` and `g1/logs/*.jsonl`. `w1/lab/realtime.py` replays rows in timestamp order, updates rolling detector state, emits metric/log-template alerts, and writes RCA JSON artifacts.

Production would swap local files for OTel, Kafka, Flink and hot stores, but the RCA logic stays conceptually the same.

## Slide 16 - Simulating The Data Flow

The dashboard demo flow has four stages:

1. Replay: read metrics and log-template events in timestamp order.
2. Gate: apply baseline thresholds, persistence and log-template count gates.
3. Correlate: merge cart memory/GC/cache/OOM signals with downstream timeout/5xx.
4. Present: dashboard shows alert stream, RCA timeline and ranked hypothesis.

Key artifacts: `events.jsonl`, `alerts.jsonl`, `signals.json`, `rca_timeline.json`, `rca_hypotheses.json`.

## Slide 17 - ADR-Style Decisions

Giải thích theo ADR-lite:

Context: nhiều service, telemetry burst, cần RCA replay.
Decision: OTel cho collection, Kafka cho transport, Flink cho stream processing, MAD+IF cho detection, Drain3 cho logs, VM/Loki/S3 cho storage.
Trade-offs: thêm operational complexity, IF kém explainable hơn MAD, nhiều storage cần vận hành.

## Slide 18 - Final Takeaway

Final takeaway: metrics answer WHEN, log templates answer WHERE, evidence ordering supports WHAT. Prevention nên bắt heap/cache pressure trước OOM bằng composite alert: memory slope + GC pause + cache eviction template count + cart latency/5xx.
