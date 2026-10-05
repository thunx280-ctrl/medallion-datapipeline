-- init_db.sql
-- Phase 0 · Task 0.1 — Khởi tạo database & 3 schema Medallion

-- 1. Ba schema chính
CREATE SCHEMA IF NOT EXISTS schema_bronze;   -- = layer "raw":     nguyên bản từ nguồn
CREATE SCHEMA IF NOT EXISTS schema_silver;   -- = layer "staging": đã clean, đúng kiểu
CREATE SCHEMA IF NOT EXISTS schema_gold;     -- = layer "dwh":     star schema cho Power BI

-- 2. Bảng log ingestion (Bronze) — mỗi loader script sẽ ghi 1 dòng ở đây mỗi lần chạy
CREATE TABLE IF NOT EXISTS schema_bronze.ingest_log (
    log_id          BIGSERIAL PRIMARY KEY,
    batch_id        UUID NOT NULL,
    source_name     TEXT NOT NULL,          -- vd: sales_transactions
    source_file     TEXT NOT NULL,          -- vd: SRC01_sales_transactions.csv
    source_platform TEXT NOT NULL,          -- 'google_drive' | 'onedrive'
    rows_loaded     INTEGER,
    status          TEXT NOT NULL,          -- 'SUCCESS' | 'FAILED'
    error_message   TEXT,
    duration_sec    NUMERIC(10,2),
    started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMP
);

-- 3. Bảng đặc biệt cho SRC-02 sales_target_plan (multi-version, Task 1.4)
--    Các bảng schema_bronze.* khác được tự tạo bởi pandas.to_sql() khi loader
--    chạy lần đầu, nhưng 2 bảng này tạo tay trước để kiểm soát rõ cấu trúc.
-- Metadata: mỗi dòng = 1 sheet (= 1 version) đã từng được nạp
CREATE TABLE IF NOT EXISTS schema_bronze.sales_target_files (
    id              BIGSERIAL PRIMARY KEY,
    source_file     TEXT NOT NULL,      -- vd: SRC02_sales_target_plan.xlsx
    sheet_name      TEXT NOT NULL,      -- vd: Plan_v1_Original
    version_label   TEXT,               -- vd: v1 (tách từ sheet_name hoặc cột plan_version)
    row_count       INTEGER,
    _ingested_at    TIMESTAMP NOT NULL DEFAULT NOW(),
    _batch_id       UUID NOT NULL
);

-- Dữ liệu thô dạng "long" của TẤT CẢ phiên bản — KHÔNG được ghi đè version cũ
CREATE TABLE IF NOT EXISTS schema_bronze.sales_targets_raw (
    id                  BIGSERIAL PRIMARY KEY,
    plan_version        TEXT,
    version_date        TEXT,
    effective_from      TEXT,
    effective_to        TEXT,
    employee_id         TEXT,
    employee_name       TEXT,
    region              TEXT,
    team                TEXT,
    year                TEXT,
    month               TEXT,
    target_revenue      TEXT,
    target_quantity     TEXT,
    target_new_customers TEXT,
    _source_file        TEXT NOT NULL,
    _sheet_name         TEXT NOT NULL,
    _source_platform    TEXT NOT NULL,
    _ingested_at        TIMESTAMP NOT NULL DEFAULT NOW(),
    _batch_id           UUID NOT NULL
);


-- Phase 2 · Task 2.1 — Bảng lưu kết quả Data Profiling của Bronze layer
CREATE TABLE IF NOT EXISTS schema_bronze.profiling_summary (
    id              BIGSERIAL PRIMARY KEY,
    table_name      TEXT NOT NULL,       -- vd: customers
    column_name     TEXT NOT NULL,       -- vd: customer_id
    dtype           TEXT,                -- kiểu dữ liệu pandas đọc được (object, datetime64...)
    total_rows      INTEGER,             -- tổng số dòng của BẢNG (lặp lại ở mỗi dòng cột cho dễ query)
    null_count      INTEGER,             -- số ô rỗng/NULL của CỘT này
    null_pct        NUMERIC(5,2),        -- % null = null_count / total_rows * 100
    distinct_count  INTEGER,             -- số giá trị duy nhất (không tính rỗng)
    profiled_at     TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Mỗi lần chạy lại profilling.py cho 1 bảng, xoá kết quả cũ của bảng đó
-- trước khi insert mới (tránh kết quả cũ/mới lẫn lộn). Lệnh xoá này nằm
-- trong profilling.py, không cần chạy tay ở đây.

-- CHECK — sau khi chạy profilling.py, kiểm tra nhanh:
-- SELECT table_name, COUNT(*) AS so_cot, MAX(total_rows) AS so_dong
-- FROM schema_bronze.profiling_summary
-- GROUP BY table_name
-- ORDER BY table_name;

-- Xem cột nào có % null cao nhất (ưu tiên xử lý ở Silver):
-- SELECT table_name, column_name, null_pct
-- FROM schema_bronze.profiling_summary
-- WHERE null_pct > 0
-- ORDER BY null_pct DESC
-- LIMIT 20;


-- .. Bảng log tổng cho main.py (mỗi lần chạy full pipeline Bronze->Silver->Gold)
-- CREATE TABLE IF NOT EXISTS schema_gold.pipeline_logs (
--     run_id          BIGSERIAL PRIMARY KEY,
--     batch_id        UUID NOT NULL,
--     pipeline_stage  TEXT NOT NULL,          -- 'BRONZE' | 'SILVER' | 'GOLD'
--     status          TEXT NOT NULL,          -- 'SUCCESS' | 'FAILED'
--     message         TEXT,
--     started_at      TIMESTAMP NOT NULL DEFAULT NOW(),
--     finished_at     TIMESTAMP,
--     duration_sec    NUMERIC(10,2)
-- );
