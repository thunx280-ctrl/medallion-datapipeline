# Data Dictionary — VietDist Analytics Platform

Tài liệu này mô tả toàn bộ dữ liệu trong pipeline: nguồn gốc, ý nghĩa nghiệp vụ, và cấu trúc từng bảng ở 3 layer Bronze → Silver → Gold. Đọc tài liệu này trước khi viết SQL/DAX mới, hoặc khi cần giải thích project cho người khác (phỏng vấn, review portfolio).

---

## 1. Bối cảnh nghiệp vụ (Business Context)

**Công ty**: VietDist Corporation — công ty phân phối FMCG (hàng tiêu dùng nhanh) giả định, hoạt động qua 2 kênh chính: bán trực tiếp (Sales) và qua nhà phân phối (Distributor).

**Mục tiêu platform**: xây dựng nền tảng phân tích Sales Performance & Distribution để trả lời các câu hỏi nghiệp vụ:
- Doanh thu thực tế so với chỉ tiêu (target) theo từng nhân viên, từng tháng, từng vùng như thế nào?
- Nhà phân phối nào giao hàng đúng hẹn/đủ số lượng tốt nhất (fill rate, on-time delivery)?
- Tỷ lệ trả hàng (return rate) theo sản phẩm/khách hàng/nhân viên ra sao?
- Khi nhân viên đổi vùng phụ trách giữa năm, doanh số các giai đoạn trước/sau có bị gán sai không? (đây là lý do bắt buộc dùng SCD Type 2)

**Đối tượng sử dụng dữ liệu**: Intern/Junior Analytics Engineer (bản thân project), và các stakeholder dùng Power BI dashboard xây trên `schema_gold`.

---

## 2. Nguồn dữ liệu (10 nguồn → Bronze layer)

| # | Nguồn | Định dạng | Mô tả nghiệp vụ | Bảng Bronze |
|---|---|---|---|---|
| 1 | Sales transactions | CSV | Đơn hàng bán trực tiếp cho khách hàng, ~119K dòng, **grain = 1 dòng sản phẩm/đơn** (1 đơn có nhiều sản phẩm) | `sales_transactions` |
| 2 | Sales target plan | XLSX | Chỉ tiêu doanh số giao cho từng nhân viên theo tháng. **Nhiều phiên bản** (v1, v2, v3...) do điều chỉnh giữa năm — mỗi version là 1 sheet riêng, không bao giờ ghi đè | `sales_targets_raw` |
| 3 | Customer master | CSV | Danh sách 2.000 khách hàng mua trực tiếp | `customers` |
| 4 | Product master | XLSX | Danh mục 100 sản phẩm (SKU) | `products` |
| 5 | Distributor orders | XLSX/XLSB | Đơn đặt hàng của nhà phân phối với công ty, ~36K dòng, grain = 1 dòng sản phẩm/đơn | `distributor_orders` |
| 6 | Distributor master | CSV | Danh sách 138 nhà phân phối | `distributors` |
| 7 | Employee master | XLSX | Nhân viên kinh doanh, **có lịch sử đổi vùng/team** (cột `effective_date`, `transfer_note`) | `employees` |
| 8 | Territory mapping | XLSX | Ánh xạ nhân viên nào phụ trách khách hàng nào theo từng thời kỳ | `territory_mapping` |
| 9 | Return transactions | CSV | Giao dịch trả hàng, 3.665 dòng | `return_transactions` |
| 10 | Promotion program | XLSX | Chương trình khuyến mãi, áp dụng cho **nhiều sản phẩm** (danh sách product_id ngăn cách bởi `\|`) | `promotion_program` |

**Quy tắc Bronze**: mọi cột đều lưu dạng `TEXT` (giữ nguyên bản gốc, không xử lý), `append`-only (không ghi đè), kèm 4 cột metadata: `_source_file`, `_source_platform`, `_ingested_at`, `_batch_id`.

---

## 3. Silver layer — dữ liệu đã làm sạch

Mỗi bảng Silver là bản Bronze tương ứng đã: loại trùng lặp (`ROW_NUMBER()`), chuẩn hoá text (`TRIM`, `UPPER`/`INITCAP`), ép kiểu đúng (`DATE`, `NUMERIC`). Tên bảng: `silver_<tên_nguồn>` (10 bảng + 1 bảng cầu nối `stg_promotion_products` tách danh sách sản phẩm khuyến mãi thành quan hệ nhiều-nhiều chuẩn).

**2 điểm nghiệp vụ quan trọng phát hiện qua profiling**:
- `sales_transactions` và `distributor_orders`: **khoá dedup là `(order_id, product_id)`**, không phải `order_id` — vì 1 đơn có nhiều dòng sản phẩm.
- `customers.tax_code` null ~52% — **bình thường** (khách cá nhân không có mã số thuế, chỉ doanh nghiệp mới có), Silver giữ NULL thật, không điền giá trị mặc định.

---

## 4. Gold layer — Star Schema

### 4.1 Dimension tables

#### `dim_date` — không phụ thuộc nguồn nào, sinh bằng `generate_series()`
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `date_id` (PK) | INT | Khoá chính dạng `YYYYMMDD` (vd `20240315`) |
| `full_date` | DATE | Ngày đầy đủ |
| `year`, `quarter`, `month`, `day` | INT | Thành phần ngày |
| `week_of_year`, `day_of_week` | INT | Tuần trong năm, thứ trong tuần (ISO: T2=1...CN=7) |
| `day_name`, `month_name` | TEXT | Tên thứ/tháng |
| `year_month`, `year_quarter` | TEXT | Nhãn gộp tiện `GROUP BY` (vd "2024-03", "Q1-2024") |
| `is_weekend` | BOOLEAN | Thứ 7/Chủ nhật |
| `is_first_day_of_month`, `is_last_day_of_month` | BOOLEAN | Cờ đầu/cuối tháng |

#### `dim_customers` — **SCD Type 1** (ghi đè, không giữ lịch sử)
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `customer_sk` (PK) | INT | Surrogate key — fact table join qua cột này |
| `customer_id` (business key) | TEXT | Mã khách hàng gốc từ nguồn |
| `customer_name`, `customer_type`, `channel`, `province`, `region`, `address`, `phone` | TEXT | Thông tin khách hàng |
| `tax_code` | TEXT | Mã số thuế — **NULL hợp lệ** với khách cá nhân |
| `join_date` | DATE | Ngày trở thành khách hàng |
| `credit_limit` | NUMERIC | Hạn mức tín dụng |
| `status` | TEXT | Active/Inactive |

#### `dim_products` — **SCD Type 1**
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `product_sk` (PK) | INT | Surrogate key |
| `product_id` (business key) | TEXT | Mã sản phẩm gốc |
| `product_name`, `category`, `sub_category`, `unit` | TEXT | Thông tin mô tả |
| `unit_price`, `cost_price` | NUMERIC | Giá bán / giá vốn |
| `weight_gram` | NUMERIC | Trọng lượng |
| `status`, `launch_date` | TEXT / DATE | Trạng thái, ngày ra mắt |

#### `dim_employees` — **SCD Type 2** (giữ lịch sử khi đổi vùng/team/chức vụ)
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `employee_sk` (PK) | INT | Surrogate key — **1 employee_id có thể có nhiều dòng** (nhiều version) |
| `employee_id` (business key) | TEXT | Mã nhân viên gốc — **KHÔNG unique** ở bảng này |
| `full_name`, `gender`, `date_of_birth`, `join_date`, `email`, `phone` | — | Thông tin cá nhân (không kích hoạt version mới nếu đổi) |
| `position`, `region`, `team`, `status` | TEXT | **Cột được theo dõi SCD2** — đổi 1 trong 4 cột này → tạo version mới |
| `effective_date` | DATE | Ngày version này bắt đầu hiệu lực |
| `expiry_date` | DATE | Ngày version này hết hiệu lực (`NULL` = đang hiệu lực) |
| `is_current` | BOOLEAN | `TRUE` = bản ghi hiện hành |

#### `dim_distributors` — **SCD Type 2** (theo dõi `tier`, `region`, `status`, `assigned_supervisor_id`)
Cấu trúc tương tự `dim_employees`, thêm: `tier`, `channel`, `province`, `contact_person`, `tax_code`, `credit_limit`, `assigned_supervisor_id`.

---

### 4.2 Fact tables

#### `fact_sales` — grain: 1 dòng sản phẩm trong 1 đơn hàng (`order_id`, `product_id`)
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `order_id`, `product_id` | TEXT | Degenerate dimension (giữ nguyên, không tách bảng riêng) |
| `date_id`, `customer_sk`, `product_sk`, `employee_sk` | INT (FK) | Khoá join tới dimension — `employee_sk` join **point-in-time** theo `order_date` |
| `channel`, `region`, `province`, `delivery_status`, `payment_method`, `payment_status` | TEXT | Thuộc tính lọc nhanh |
| `quantity`, `unit_price`, `discount_pct`, `discount_amount`, `gross_amount`, `net_amount` | NUMERIC | **Measures** — `net_amount` là doanh thu thực nhận sau chiết khấu |

#### `fact_targets` — grain: 1 nhân viên - 1 tháng - 1 **version kế hoạch**
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `plan_version`, `version_date`, `effective_from`, `effective_to` | — | Thông tin version — **mọi version đều giữ lại**, không lọc bản mới nhất ở đây |
| `date_id` | INT | Ngày đầu tháng của kỳ target (vd tháng 3/2024 → `20240301`) |
| `employee_sk` | INT (FK) | Join point-in-time theo `effective_from` |
| `target_revenue`, `target_quantity`, `target_new_customers` | NUMERIC | Measures (chỉ tiêu) |

#### `fact_returns` — grain: 1 `return_id`
| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `return_id`, `original_order_id` | TEXT | Định danh |
| `date_id`, `customer_sk`, `product_sk`, `employee_sk` | INT (FK) | Join point-in-time theo `return_date` |
| `return_reason`, `status` | TEXT | Lý do trả, trạng thái xử lý |
| `return_quantity`, `unit_price`, `return_amount` | NUMERIC | Measures |

---

### 4.3 Mart tables (tổng hợp sẵn cho BI)

#### `mart_sales_vs_target` — grain: 1 nhân viên - 1 tháng
So sánh doanh thu thực tế (đã trừ trả hàng) với chỉ tiêu — **chỉ lấy version target mới nhất** cho mỗi nhân viên-tháng (chọn qua `ROW_NUMBER()` theo `version_date`).

| Cột | Ý nghĩa |
|---|---|
| `target_revenue` | Chỉ tiêu doanh thu (version mới nhất) |
| `gross_actual_revenue` | Doanh thu bán hàng thô (từ `fact_sales`, chưa trừ trả hàng) |
| `actual_return_amount` | Tổng giá trị trả hàng trong kỳ (từ `fact_returns`) |
| `actual_revenue` | **= `gross_actual_revenue` − `actual_return_amount`** (doanh thu ròng, dùng để đánh giá KPI) |
| `revenue_gap` | `actual_revenue` − `target_revenue` |
| `revenue_achievement_pct` | `actual_revenue` / `target_revenue` × 100 |

#### `mart_distributor_performance` — grain: 1 nhà phân phối - 1 tháng
| Cột | Ý nghĩa |
|---|---|
| `total_orders` | Số đơn đặt hàng |
| `total_qty_ordered` / `total_qty_delivered` | Số lượng đặt / đã giao |
| `avg_fill_rate_pct` | % giao đủ hàng trung bình (thang 0-100, cần chia 100 khi làm DAX `%`) |
| `ontime_delivery_rate_pct` | % giao đúng hẹn (thang 0-100) |

---

## 5. Quy ước đặt tên & khoá

- **Surrogate key** (`*_sk`): số nguyên tự sinh, dùng để fact/mart JOIN dimension — áp dụng nhất quán cho **mọi** dimension (kể cả SCD1) để toàn schema có cách join giống nhau.
- **Business key** (`*_id`): mã gốc từ hệ thống nguồn — **không unique** ở dimension SCD2 (1 business key có nhiều dòng lịch sử).
- **Point-in-time join**: mọi fact join dimension SCD2 theo điều kiện `transaction_date BETWEEN effective_date AND expiry_date`, **không** theo `is_current = TRUE` — đảm bảo giao dịch quá khứ luôn gán đúng phiên bản dimension tại thời điểm đó.
