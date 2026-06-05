# SUBMIT

## Group Reflection

Nhóm xử lý sự cố như một bài toán sắp xếp bằng chứng thay vì bắt đầu từ alert text. Nhóm đã kiểm tra hình dạng telemetry, số dòng, khoảng thời gian timestamp, bản ghi trùng lặp, khoảng trống dữ liệu, null, và hành vi baseline. Sau đó nhóm so sánh robust MAD detector, EWMA trend smoothing, và IsolationForest. Robust MAD là phương án dễ bảo vệ nhất vì mỗi anomaly đều gắn với service và metric cụ thể cùng ngưỡng baseline rõ ràng. IsolationForest được dùng như kiểm tra phụ, còn EWMA giúp giải thích xu hướng và thời điểm theo trực quan. Với logs, nhóm hiệu chỉnh template extraction để các message quan trọng không bị gom vào một lỗi upstream chung. Timeline kết quả cho thấy cart-service chịu áp lực memory và GC, cache eviction failures, OOMKilled events, tăng restart, rồi lan sang downstream timeout/5xx. Bài học chính là các tín hiệu vận hành xuất hiện sớm trước khi page, nhưng cần liên kết metrics và template-level logs thay vì đọc từng alert rời rạc.

## Contributions

- Đinh Danh Nam: Anomaly Detection + RCA.
- Huỳnh Nguyễn Ngọc Tân: EDA.
- Cái Xuân Hoà: Build data pipeline.
- Nguyễn Trần Huy Vũ: Build streaming -> ingestion -> RCA pipeline.
- Huỳnh Xuân Hậu: Anomaly Detection + RCA.
- Nguyễn Tất Văn: Anomaly Detection + RCA.
- Trần Đình Thông: EDA.
- Lê Ngọc Thành Tâm: Build data pipeline.
