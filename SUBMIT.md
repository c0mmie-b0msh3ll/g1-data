# SUBMIT

## Group Reflection

Nhóm đã tiến hành tìm và truy vết hệ thống khi xảy ra sự cố. Nhóm đã kiểm tra thông tin của telemetry, số dòng, khoảng thời gian metrics được thu thập, missing data, và baseline behaviour trong 6 giờ đầu tiên trong ngày. Sau đó nhóm so sánh robust MAD detector, EWMA trend smoothing, và Isolation Forest để lựa chọn phương án phù hợp nhất với datasets và bài toán hiện tại. Robust MAD là phương án có sự chính xác ổn nhất vì mỗi anomaly đều gắn với service và metric cụ thể cùng ngưỡng baseline rõ ràng. Isolation Forest được dùng để cross confirmation và nhóm không lựa chọn do khó giải thích từng metric riêng biệt, còn EWMA giúp giải thích xu hướng. Với logs, nhóm đã hiệu chỉnh template extraction để các message quan trọng không bị gom vào một template. Timeline kết quả cho thấy cart-service chịu áp lực memory và GC, cache eviction failures, OOMKilled events, tăng restart, rồi lan sang downstream timeout/5xx. Bài học mà nhóm đã rút ra được là các tín hiệu vận hành xuất hiện sớm và trước khi xảy ra sự cố và alert, nhưng để hiểu rõ root causes nhằm đưa ra giải pháp phù hợp thì cần phải kết hợp metrics anomaly và template logs thay vì đọc từng alert rời rạc.

## Contributions

- Đinh Danh Nam: Anomaly detection with Isolation Forest
- Huỳnh Nguyễn Ngọc Tân: Loading data, data missing checking, Anomaly detection with EWMA
- Cái Xuân Hoà: Anomaly detection with MAD
- Nguyễn Trần Huy Vũ: Log parsing with Drain3
- Huỳnh Xuân Hậu: Complete Findings.md
- Nguyễn Tất Văn: Template spike analysis trong khoảng thời gian có anomaly
- Trần Đình Thông: Drill down tìm root cause của template bị spike mạnh nhất, Visualize anomaly detection
- Lê Ngọc Thành Tâm: Calculating statistics (mean, std, skewness, min, max)
