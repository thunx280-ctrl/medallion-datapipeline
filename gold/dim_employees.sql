-- =====================================================================
-- dim_employees.sql
-- Gold layer: schema_silver.employees -> schema_gold.dim_employees
-- Loại: SCD TYPE 2 (giữ lịch sử) — khi nhân viên đổi region/team/position,
-- KHÔNG ghi đè, mà ĐÓNG bản ghi cũ (set expiry_date) và MỞ bản ghi mới.
-- Lý do cần giữ lịch sử: nếu nhân viên A chuyển từ vùng Bắc sang vùng
-- Nam vào tháng 6, doanh số tháng 1-5 phải tính cho vùng Bắc, doanh số
-- tháng 7-12 phải tính cho vùng Nam — nếu ghi đè, toàn bộ lịch sử sẽ bị
-- gán sai về vùng Nam.
--
-- ⚠️ KHÁC BIỆT QUAN TRỌNG so với dim_customers/dim_products:
-- KHÔNG dùng DROP TABLE + CREATE TABLE AS SELECT (như 2 dim trước), vì
-- làm vậy sẽ XOÁ SẠCH LỊCH SỬ đã tích luỹ qua các lần chạy trước. Thay
-- vào đó dùng CREATE TABLE IF NOT EXISTS (chỉ tạo ở lần chạy đầu tiên)
-- + UPDATE (đóng bản ghi cũ) + INSERT (mở bản ghi mới) mỗi lần chạy lại.
--
-- CÁC CỘT ĐƯỢC THEO DÕI THAY ĐỔI (SCD2 columns) — thay đổi ở BẤT KỲ cột
-- nào trong nhóm này sẽ tạo ra 1 version mới: region, team, position, status.
-- Các cột còn lại (full_name, phone, email...) được coi là ít thay đổi/
-- không quan trọng cho phân tích lịch sử, nên KHÔNG kích hoạt version mới
-- (nếu chúng thay đổi cùng lúc với 1 cột được theo dõi, giá trị mới vẫn
-- được ghi nhận vào version mới - chỉ là bản thân chúng không PHẢI lý do
-- tạo version).
-- =====================================================================

-- BƯỚC 0: Tạo bảng (chỉ chạy thật sự ở LẦN ĐẦU TIÊN; các lần sau
-- IF NOT EXISTS sẽ bỏ qua, không đụng vào dữ liệu đã có)
CREATE TABLE IF NOT EXISTS schema_gold.dim_employees (
    employee_sk     BIGSERIAL PRIMARY KEY,
    employee_id     TEXT NOT NULL,
    full_name       TEXT,
    gender          TEXT,
    date_of_birth   DATE,
    join_date       DATE,
    "position"      TEXT,
    region          TEXT,
    team            TEXT,
    email           TEXT,
    phone           TEXT,
    status          TEXT,
    effective_date  DATE NOT NULL,     -- ngày version này bắt đầu hiệu lực
    expiry_date     DATE,              -- NULL = đang hiệu lực; có giá trị = đã hết hiệu lực
    is_current      BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at      TIMESTAMP NOT NULL DEFAULT NOW()
);

-- BƯỚC 1: ĐÓNG các bản ghi hiện hành có thay đổi ở cột được theo dõi
-- expiry_date = ngày hiệu lực mới trừ 1 ngày (ngày cuối cùng version cũ còn đúng)
UPDATE schema_gold.dim_employees de
SET
    expiry_date = COALESCE(e.effective_date, CURRENT_DATE) - 1,
    is_current  = FALSE
FROM schema_silver.employees e
WHERE de.employee_id = e.employee_id
  AND de.is_current = TRUE
  AND (
        COALESCE(de.region, '')   <> COALESCE(e.region, '')
     OR COALESCE(de.team, '')     <> COALESCE(e.team, '')
     OR COALESCE(de."position", '') <> COALESCE(e."position", '')
     OR COALESCE(de.status, '')   <> COALESCE(e.status, '')
  );

-- BƯỚC 2: MỞ bản ghi mới cho:
--   (a) nhân viên hoàn toàn mới (chưa từng có trong dim_employees)
--   (b) nhân viên vừa bị đóng ở BƯỚC 1 (vì sau bước 1, họ không còn dòng
--       is_current=TRUE nào -> LEFT JOIN...WHERE de.employee_id IS NULL
--       sẽ bắt được cả 2 trường hợp (a) và (b) chỉ bằng 1 điều kiện)
INSERT INTO schema_gold.dim_employees (
    employee_id, full_name, gender, date_of_birth, join_date, "position",
    region, team, email, phone, status, effective_date, expiry_date, is_current
)
SELECT
    e.employee_id, e.full_name, e.gender, e.date_of_birth, e.join_date, e."position",
    e.region, e.team, e.email, e.phone, e.status,
    COALESCE(e.effective_date, CURRENT_DATE)   AS effective_date,
    NULL                                        AS expiry_date,
    TRUE                                        AS is_current
FROM schema_silver.employees e
LEFT JOIN schema_gold.dim_employees de
    ON e.employee_id = de.employee_id AND de.is_current = TRUE
WHERE de.employee_id IS NULL;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.dim_employees WHERE is_current = TRUE; -- kỳ vọng = 114 (= số nhân viên hiện tại trong silver_employees)
-- SELECT employee_id, COUNT(*) FROM schema_gold.dim_employees WHERE is_current = TRUE GROUP BY employee_id HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng (không được có 2 bản ghi is_current cùng lúc cho 1 nhân viên)
-- -- Chạy lại file này LẦN 2 ngay (không đổi gì trong slver_employees) rồi chạy lại 2 câu trên -> kết quả PHẢI GIỐNG HỆT lần 1 (kiểm tra tính idempotent)
-- SELECT employee_id, region, team, effective_date, expiry_date, is_current
-- FROM schema_gold.dim_employees ORDER BY employee_id, effective_date; -- xem lịch sử đầy đủ của từng nhân viên
