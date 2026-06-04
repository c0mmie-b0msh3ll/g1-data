# AIOps W1 Lab — Detect & Triage: "Cứu hệ thống đang cháy"

**Week:** AIOps W1 (Program W8)
**Format:** Group (7-8 students) | Thu–Fri full day
**Submit by:** End of Friday (EOD)
**Repo path:** `aiops-{group-id}/w1/lab/` — e.g. `aiops-g1/w1/lab/`

ShopX là một nền tảng thương mại điện tử Việt Nam chạy trên Kubernetes. Stack gồm sáu microservice: `api-gateway`, `product-service`, `cart-service`, `order-service`, `payment-service`, và `notification-service`. Metrics được Prometheus scrape mỗi 30 giây; logs chảy qua Fluentd vào Elasticsearch.

Một tối thứ Tư gần đây, on-call engineer nhận được một loạt alert lúc 23:04:

```
[CRITICAL] cart-service:    HTTP 5xx rate = 34%   (threshold: 5%)
[CRITICAL] cart-service:    pod restart count = 7 in last 2h
[WARNING]  order-service:   upstream timeout rate = 28%
[WARNING]  payment-service: upstream timeout rate = 12%
```

Engineer đã restart toàn bộ `cart-service` pods. Hệ thống ổn định trở lại. Nhưng không ai biết chính xác chuyện gì đã xảy ra, khi nào nó bắt đầu, hay tại sao lại có restart loop.

**Nhiệm vụ của nhóm:** Phân tích 24 giờ telemetry (metrics + logs) kết thúc tại thời điểm incident được suppressed. Trả lời ba câu hỏi:

1. **WHEN** — Anomaly bắt đầu từ khi nào? Có "silent signal" sớm hàng giờ trước khi alerts hiển thị không?
2. **WHERE** — Service nào, metric nào, log pattern nào là chỉ báo sớm nhất?
3. **WHAT** — Root cause hypothesis của nhóm là gì? Cơ chế nào gây ra restart loop?

---

## Dataset

Dataset nằm trong thư mục `data/` của repo template. Không commit data — đã có trong `.gitignore`.

### Metrics

Bốn file CSV, mỗi file có cột `timestamp` (ISO 8601, UTC, interval 30 giây, 24 giờ → ~2,880 rows).

**`data/metrics/cart-service.csv`**

| Column | Unit | Description |
|--------|------|-------------|
| `timestamp` | ISO 8601 | `2026-06-01T00:00:00Z`, every 30s |
| `memory_usage_bytes` | bytes | Container RSS memory |
| `memory_limit_bytes` | bytes | Kubernetes memory limit (2 GB) |
| `cpu_usage_percent` | % | CPU usage 0–100 |
| `http_requests_per_sec` | req/s | Inbound request rate |
| `http_p99_latency_ms` | ms | P99 response latency |
| `http_5xx_rate` | % | Fraction of responses that are 5xx |
| `jvm_gc_pause_ms_avg` | ms | Average GC pause per 30s window |
| `container_restart_count` | count | Cumulative pod restart counter |

**`data/metrics/order-service.csv`** and **`data/metrics/payment-service.csv`**

Columns: `timestamp`, `http_requests_per_sec`, `http_p99_latency_ms`, `http_5xx_rate`, `upstream_timeout_rate`

**`data/metrics/api-gateway.csv`**

Columns: `timestamp`, `http_requests_per_sec`, `http_p99_latency_ms`, `http_5xx_rate`, `cart_upstream_error_rate`, `active_connections`

### Logs

**`data/logs/cart-service.log.jsonl`** — ~24,000 lines, one JSON object per line. Fields: `timestamp`, `level` (INFO/WARN/ERROR/FATAL), `service`, `pod`, `trace_id`, `message`, plus optional extra fields per log type.

**`data/logs/order-service.log.jsonl`** — ~8,000 lines, same schema.

---

## Expected Output

Khi hoàn thành, nhóm cần có:

- **Code** có thể chạy lại được từ raw data đến kết quả — notebook, script, hoặc cả hai.
- **Anomaly detection** với ít nhất hai phương pháp khác nhau và so sánh hiệu quả của chúng.
- **Log analysis** trích xuất signal từ log patterns — không chỉ đọc raw lines.
- **FINDINGS.md** — postmortem kỹ thuật trả lời đủ WHEN / WHERE / WHAT, có timestamp cụ thể, evidence từ cả metrics lẫn logs.
- **SUBMIT.md** — group reflection (≥150 words) và một câu ghi contribution của từng thành viên.

---

