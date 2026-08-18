# Architecture Decision Record (ADR): LLM Observability at Scale (1B Requests/Day)

**Author:** Nguyen Vu Ha An  
**Role:** Lakehouse Platform Architect  
**Status:** Approved for Review  
**Target Delivery:** Q3/2026  

---

## 1. Problem Statement

Xây dựng hệ thống Data Lakehouse thu thập và giám sát toàn bộ request/response của Foundation-Model API với quy mô:
* **Scale:** 1 tỷ (1,000,000,000) requests/ngày ($\approx 11,600\text{ req/s}$ trung bình, đỉnh điểm $25,000\text{ req/s}$).
* **Payload:** Trung bình 5 KB/req (Prompt + Completion + Token counts + Timings) $\rightarrow$ **5 TB raw data/ngày** (150 TB/tháng).

### Ràng buộc kỹ thuật & nghiệp vụ:
1. **SLA Dashboard:** Cập nhật chi phí & latency theo từng Tenant theo chu kỳ **5 phút** ($p95 < 2\text{s}$).
2. **Data Lifecycle:** Toàn bộ raw prompt/response lưu đầy đủ trong **7 ngày** để phục vụ Incident Debugging; sau 7 ngày chỉ giữ lại aggregated metrics trong **1 năm**.
3. **Privacy & Compliance:** Redact/Tokenize toàn bộ thông tin định danh cá nhân (PII) trước khi ghi xuống tầng cho phép Analyst/Engineer truy vấn.
4. **Hard FinOps Cap:** Tổng ngân sách storage trên Cloud **$\le \$5,000/\text{tháng}$**.

### Vì sao bài toán này khó?
Nếu ghi thẳng 1B requests nhỏ vào S3 sẽ tạo ra **1 tỷ file/ngày (Small-File Disaster)**, làm sập mọi query engine và tốn hàng chục ngàn USD tiền PUT requests. Nếu dùng S3 Standard lưu trữ 150 TB raw/tháng, riêng tiền storage thuần đã ngốn $150 \times \$23 = \$3,450/\text{tháng}$, chưa tính chi phí truy vấn và compute compaction.

---

## 2. Architecture Diagram

```
                       [ 1B Requests / Day (25K req/s peak) ]
                                        │
                                        ▼
                   ┌─────────────────────────────────────────┐
                   │   API Gateway & Event Streaming Queue   │
                   │           (Apache Kafka / Redpanda)     │
                   └────────────────────┬────────────────────┘
                                        │
                         Streaming Ingestion & Tokenizer
                          (Rust-based Worker / Flink)
                                        │
               ┌────────────────────────┴────────────────────────┐
               │ [Dual-Write Split]                              │
               ▼                                                 ▼
┌───────────────────────────────┐               ┌─────────────────────────────────┐
│     S3 Raw Blobs Bucket       │               │      Bronze Ingestion Table     │
│   (Gzip Payload: Prompt/Resp) │               │      (Delta Lake - Partitioned) │
│   • Key: tenant/date/req_id   │               │   • Metadata, Tokenized Hashes, │
│   • S3 Lifecycle: Expire = 7d │               │     Latency, Token Counts       │
└───────────────────────────────┘               └────────────────┬────────────────┘
                                                                 │
                                                   Hourly Bin-Packing & Compaction
                                                   dt.optimize.compact(target=256MB)
                                                   dt.optimize.z_order(["tenant", "ts"])
                                                                 │
                                                                 ▼
                                                ┌─────────────────────────────────┐
                                                │       Silver Governed Table     │
                                                │      (Cleaned & PII-Redacted)   │
                                                │   • Partition: event_date       │
                                                │   • Z-Order: tenant_id, model   │
                                                │   • Retention: 30d Standard,    │
                                                │     90d IA, 365d Glacier IR     │
                                                └────────────────┬────────────────┘
                                                                 │
                                                   5-Min Micro-batch Aggregation
                                                   (Incremental append via CDF)
                                                                 │
                                                                 ▼
                                                ┌─────────────────────────────────┐
                                                │       Gold Analytics Table      │
                                                │   (Rollups: 5m, 1h, 1d metrics) │
                                                │   • Cost, p50/p95 latency, TPS  │
                                                └────────────────┬────────────────┘
                                                                 │
                                                  Zero-Copy Embedded SQL (DuckDB)
                                                                 │
                                                                 ▼
                                                ┌─────────────────────────────────┐
                                                │   FinOps & Tenant SLO Dashboard │
                                                │   (Grafana / BI Tool - p95 < 1s)│
                                                └─────────────────────────────────┘
```

---

## 3. Quyết định Kiến trúc & Phân tích Đánh đổi (Trade-offs)

### Quyết định 1: Tách biệt Payload (Pointer Layout) thay vì lưu Inline
* **Lựa chọn:** **Pointer Layout (Hybrid Storage)**. Dữ liệu bảng (Silver) chỉ lưu metadata, metrics, token counts và `blob_uri` trỏ tới file S3 chứa prompt/response gzip (~400 bytes/dòng). File prompt/response thật được ghi độc lập vào S3 Bucket có cấu hình **S3 Lifecycle Expire = 7 days**.
* **Loại trừ Inline Table:** Nếu nhồi 5 KB prompt vào thẳng file Parquet, bảng sẽ nặng 5 TB/ngày. Khi dashboard quét bảng để tính sum token/latency mỗi 5 phút, nó bị *I/O amplification* gấp $\ge 12\times$ (đọc 5 TB thay vì 400 GB), làm dashboard chậm chạp và tốn chi phí I/O khổng lồ.
* **Loại trừ DynamoDB/NoSQL:** Lưu 1 tỷ request/ngày vào NoSQL tốn $\$1.25$ cho 1M write units $\rightarrow \$1,250/\text{ngày} = \$37,500/\text{tháng}$, vượt ngân sách $7.5\times$.

### Quyết định 2: Định dạng bảng Delta Lake thay vì Apache Iceberg hay Apache Hudi
* **Lựa chọn:** **Delta Lake**. Cung cấp native Z-Ordering đa chiều (`dt.optimize.z_order(["tenant_id", "timestamp"])`), Deletion Vectors (hỗ trợ Right-to-Erasure mà không cần rewrite toàn bộ Parquet), và engine `delta-rs` viết bằng Rust giúp query siêu nhẹ từ DuckDB/Python không cần JVM.
* **Loại trừ Apache Iceberg:** Rất mạnh về catalog đa nền tảng nhưng việc cập nhật metadata snapshot qua SQLite/REST Catalog ở tần suất streaming 25,000 writes/giây dễ gặp xung đột commit lock (commit conflict).
* **Loại trừ Apache Hudi:** Chi phí tính toán indexing (Bloom/HBase index) khi nạp 5 TB/ngày quá nặng, làm tăng chi phí compute cluster.

### Quyết định 3: Khử PII tại Ingestion Worker thay vì Query-Time Masking
* **Lựa chọn:** **Streaming Deterministic Tokenization (HMAC-SHA256 + Vault Salt)** ngay tại tầng Streaming Worker trước khi dữ liệu chạm Bronze Lakehouse.
* **Loại trừ Query-time Dynamic Masking:** Dễ bị bypass nếu query lọt qua các engine khác nhau, tốn CPU tính toán regex cho mỗi lần load dashboard, và vi phạm nguyên tắc "Zero Raw PII on Analytical Storage" của chuẩn kiểm toán bảo mật.
* **Loại trừ LLM Redactor:** Gọi model để redact 1 tỷ requests/ngày sẽ tốn hàng triệu USD/tháng và độ trễ nạp tăng thêm vài trăm ms.

### Quyết định 4: Hai tầng Compaction (Streaming Micro-batch + Hourly Z-Order)
* **Lựa chọn:** 
  1. *Tầng 1 (In-Memory Buffer):* Worker gom 50,000 records ($\approx 25\text{ MB}$) rồi mới ghi 1 file Parquet vào Bronze (tránh 1 request = 1 file).
  2. *Tầng 2 (Hourly Job):* Chạy `dt.optimize.compact(target_size=256MB)` và `z_order(["tenant_id", "timestamp"])` định kỳ mỗi 60 phút trên Silver.
* **Loại trừ Nightly Compaction (Chạy 1 lần ban đêm):** Để lại 24 tiếng với hàng chục ngàn file nhỏ $\rightarrow$ câu lệnh query dashboard 5 phút ban ngày sẽ quét qua file phân mảnh, vi phạm SLA $p95 < 2\text{s}$.
* **Loại trừ Streaming Direct Single-File Write:** Đòi hỏi shuffle toàn bộ cluster streaming phân tán, gây tắc nghẽn network pipeline.

### Quyết định 5: Phục vụ Dashboard qua Pre-aggregated Gold Tables + DuckDB
* **Lựa chọn:** Dùng Change Data Feed (CDF) từ Silver để tính toán sẵn bảng **Gold (Rollups 5-phút)**. Dashboard đọc trực tiếp bảng Gold qua DuckDB in-memory.
* **Loại trừ Query trực tiếp Athena vào Bronze/Silver:** AWS Athena tính phí $\$5/\text{TB}$ scan. Quét 5 TB/ngày với tần suất 5 phút/lần (288 lần/ngày) sẽ tốn $5\text{ TB} \times 288 \times \$5 = \$7,200/\text{ngày}$ (phá sản ngân sách trong 1 ngày).

---

## 4. Failure Modes & Kịch bản Ứng phó Sự cố (3:00 AM Incidents)

### Sự cố 1: Streaming Worker crash liên tục tạo ra 50,000 Uncommitted Orphan Files
* **Hiện tượng:** Bộ nhớ storage S3 tăng đột biến nhưng query trên bảng Delta không thấy tăng dòng. Số lượng file trên đĩa nhiều gấp đôi số file được commit trong `_delta_log/`.
* **Cách phát hiện:** CloudWatch Alarm so sánh `S3 Storage Lens total bytes` với `DeltaTable.detail()['size_in_bytes']`. Nếu chênh lệch $> 15\%$ trong 3 giờ liên tiếp $\rightarrow$ kích hoạt cảnh báo P1.
* **Quy trình xử lý (Rollback & Clean):**
  Chạy Job Orphan Sweeper tự động (sử dụng thuật toán hiệu tập hợp `Disk \ Log` đã kiểm chứng ở Lab 6):
  1. Liệt kê toàn bộ file trên S3 path.
  2. Liệt kê active files từ Delta log snapshot mới nhất.
  3. `rm` toàn bộ file có thời gian tạo $> 24\text{h}$ không nằm trong log.

### Sự cố 2: Poisoned Schema Evolution làm sập toàn bộ Gold Aggregation Pipeline
* **Hiện tượng:** Một provider cập nhật schema (ví dụ: `latency` từ `float` chuyển thành `string` "120ms"), pipeline Silver bắn exception, dashboard tenant đứng yên không refresh.
* **Cách phát hiện:** Dead-Letter Queue (DLQ) của Streaming Ingestion ghi nhận $> 100\text{ errors/phút}$.
* **Quy trình xử lý:**
  1. *Cách ly:* Ingestion bật Schema Enforcement nghiêm ngặt, tự động định tuyến các record sai schema vào `_lakehouse/quarantine/`.
  2. *Rollback:* Dùng Time Travel khôi phục bảng Silver về phiên bản trước lỗi:
     ```python
     dt = DeltaTable(SILVER_PATH)
     dt.restore_to_version(healthy_version)
     ```
  3. *Sửa chữa:* Áp dụng transformer chuẩn hoá type tại Bronze DLQ rồi replay lại mà không cần downtime.

### Sự cố 3: S3 API Throttling (HTTP 503 Slow Down) tại Peak Load (25,000 req/s)
* **Hiện tượng:** S3 từ chối nhận request PUT Parquet do vượt quá ngưỡng 3,500 PUT/s trên một prefix đơn lẻ.
* **Cách phát hiện:** Tỉ lệ lỗi HTTP 503 trên S3 Gateway tăng $> 0.1\%$.
* **Quy trình xử lý:**
  Áp dụng **Hash-Prefix Partitioning** cho storage bucket:
  ```
  s3://lake-storage/bronze/p_{md5(tenant_id)[0..2]}/year=2026/month=08/day=18/
  ```
  Phân tán tải ghi ra 256 prefix S3 độc lập, nâng trần chịu tải lên $256 \times 3,500 = 896,000\text{ PUT/s}$, giải quyết triệt để nghẽn cổ chai S3.

---

## 5. Ước tính Chi phí Chi tiết (Back-of-Envelope FinOps Math)

### A. Chi phí Lưu trữ (Storage on AWS S3 - us-east-1)
1. **Raw Blobs Bucket (Prompts & Responses - Giữ 7 ngày):**
   * Dung lượng: $5\text{ TB/ngày} \times 7\text{ ngày} = 35\text{ TB}$.
   * Đơn giá S3 Standard: $\$0.023/\text{GB-tháng}$.
   * **Chi phí:** $35,000\text{ GB} \times \$0.023 = \mathbf{\$805/\text{tháng}}$.

2. **Silver Table (Tabular Metadata & Metrics - Giữ 1 năm):**
   * Kích thước: 400 bytes/dòng $\times 1\text{B dòng/ngày} = 400\text{ GB/ngày} \approx 12\text{ TB/tháng}$ (Nén ZSTD $\approx 3\text{ TB/tháng}$).
   * *Tier 1 (0–30 ngày, S3 Standard):* $3\text{ TB} \times \$23 = \$69/\text{tháng}$.
   * *Tier 2 (31–90 ngày, S3 Standard-IA @ $\$0.0125$):* $6\text{ TB} \times \$12.5 = \$75/\text{tháng}$.
   * *Tier 3 (91–365 ngày, S3 Glacier Instant Retrieval @ $\$0.004$):* $27.5\text{ TB} \times \$4 = \$110/\text{tháng}$.
   * **Tổng Silver Storage:** $\$69 + \$75 + \$110 = \mathbf{\$254/\text{tháng}}$.

3. **Gold Analytics Table (Rollup metrics - Giữ 1 năm):**
   * Dung lượng: $\approx 200\text{ MB/ngày} = 6\text{ GB/tháng} \rightarrow \mathbf{<\$2/\text{tháng}}$.

$$\text{Tổng Chi Phí Storage} = \$805 + \$254 + \$2 = \mathbf{\$1,061 / \text{tháng}}$$
*(Thặng dư ngân sách: $\$5,000 - \$1,061 = \mathbf{\$3,939 / \text{tháng}}$)*.

### B. Chi phí Compute (Dành cho Compaction & Ingestion)
* Ingestion + Compaction chạy trên cụm EKS Spot Instances (Graviton ARM `c7g.xlarge` @ $\$0.05/\text{giờ}$):
  * 4 nodes $\times 720\text{ giờ} \times \$0.05 = \mathbf{\$144/\text{tháng}}$.

$$\mathbf{\text{TỔNG CHI PHÍ TOÀN HỆ THỐNG}} \approx \mathbf{\$1,205 / \text{tháng}} \quad (\text{Chiếm } 24.1\% \text{ trần ngân sách } \$5,000)$$

---

## 6. Kế hoạch Triển khai MVP 1 Tuần (1-Week Shippable Slice)

* **Mục tiêu:** Chứng minh toàn bộ pipeline hoạt động thông suốt cho 1 Tenant lớn ($10\text{M requests/ngày}$) với đầy đủ tính năng PII Masking, Compaction, và 5-Min Dashboard.

| Ngày | Hạng mục công việc (Milestone) | Tiêu chí nghiệm thu (Deliverable) |
|---|---|---|
| **Day 1** | Xây dựng Kafka Consumer + Ingestion Worker (Python/Rust). | Ghi nhận 10M events vào S3 Raw Blobs (gzip) và Bronze Delta Table với Hash Partitioning. |
| **Day 2** | Tích hợp Module Tokenizer PII (HMAC-SHA256). | Bảng Silver đảm bảo 100% không còn plain text email/tên/API Key; kiểm tra latency tokenization $< 2\text{ms/req}$. |
| **Day 3** | Cài đặt Job Compaction & Z-Order tự động (`delta-rs`). | Compaction giảm số lượng file từ 5,000 xuống còn 20 file 256MB; tốc độ point-query theo tenant tăng $\ge 5\times$. |
| **Day 4** | Xây dựng Pipeline Gold Rollups 5-phút qua DuckDB. | Bảng Gold sinh ra đúng các metrics: `total_tokens`, `cost_usd`, `p95_latency` theo từng 5 phút. |
| **Day 5** | Dựng Dashboard Grafana kết nối DuckDB + Kiểm thử Chaos. | Dashboard refresh 5s/lần không nghẽn I/O; chạy test giả lập Worker crash để kiểm tra Orphan Sweeper & Time Travel Rollback. |

---

## 7. Kết luận

Kiến trúc trên giải quyết trọn vẹn bài toán 1B requests/ngày bằng cách:
1. **Tránh Small-File & I/O Bloat** nhờ kiến trúc **Pointer Layout**.
2. **Đảm bảo SLA 5 phút** nhờ cơ chế **Hierarchical Compaction + Z-Ordering**.
3. **Tiết kiệm 76% ngân sách** thông qua **Lifecycle Tiering (Standard $\rightarrow$ IA $\rightarrow$ Glacier IR)**.
4. **An toàn vận hành** với các kịch bản phát hiện và xử lý lỗi tại 3:00 AM dựa trên các nguyên lý lõi của Lakehouse (Time Travel, Orphan Sweeping, Schema Enforcement).
