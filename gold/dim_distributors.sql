-- Gold layer: schema_silver.stg_distributors -> schema_gold.dim_distributors
-- Loại: SCD TYPE 2 — logic giống hệt dim_employees.sql, theo dõi thay đổi
-- ở: tier, region, status, assigned_supervisor_id.
--
-- KHÁC dim_employees 1 điểm: stg_distributors KHÔNG có sẵn cột
-- effective_date từ nguồn (khác employee_master có cột effective_date
-- riêng) -> dùng CURRENT_DATE làm ngày hiệu lực khi phát hiện thay đổi
-- (tức ngày pipeline chạy phát hiện thay đổi, không phải ngày thay đổi
-- thật sự xảy ra ở thực tế — đây là hạn chế thường gặp khi nguồn không
-- cung cấp effective date, chấp nhận được cho phạm vi project).
-- =====================================================================

CREATE TABLE IF NOT EXISTS schema_gold.dim_distributors (
    distributor_sk          BIGSERIAL PRIMARY KEY,
    distributor_id          TEXT NOT NULL,
    distributor_name        TEXT,
    tier                     TEXT,
    channel                  TEXT,
    province                 TEXT,
    region                   TEXT,
    contact_person           TEXT,
    phone                    TEXT,
    email                    TEXT,
    tax_code                 TEXT,
    join_date                DATE,
    credit_limit             NUMERIC,
    status                   TEXT,
    assigned_supervisor_id   TEXT,
    effective_date           DATE NOT NULL,
    expiry_date              DATE,
    is_current                BOOLEAN NOT NULL DEFAULT TRUE,
    _loaded_at                TIMESTAMP NOT NULL DEFAULT NOW()
);

-- BƯỚC 1: Đóng bản ghi hiện hành có thay đổi
UPDATE schema_gold.dim_distributors dd
SET
    expiry_date = CURRENT_DATE - 1,
    is_current  = FALSE
FROM schema_silver.distributors d
WHERE dd.distributor_id = d.distributor_id
  AND dd.is_current = TRUE
  AND (
        COALESCE(dd.tier, '')                   <> COALESCE(d.tier, '')
     OR COALESCE(dd.region, '')                  <> COALESCE(d.region, '')
     OR COALESCE(dd.status, '')                  <> COALESCE(d.status, '')
     OR COALESCE(dd.assigned_supervisor_id, '')  <> COALESCE(d.assigned_supervisor_id, '')
  );

-- BƯỚC 2: Mở bản ghi mới cho distributor mới hoặc vừa bị đóng
INSERT INTO schema_gold.dim_distributors (
    distributor_id, distributor_name, tier, channel, province, region,
    contact_person, phone, email, tax_code, join_date, credit_limit, status,
    assigned_supervisor_id, effective_date, expiry_date, is_current
)
SELECT
    d.distributor_id, d.distributor_name, d.tier, d.channel, d.province, d.region,
    d.contact_person, d.phone, d.email, d.tax_code, d.join_date, d.credit_limit, d.status,
    d.assigned_supervisor_id, COALESCE(d.join_date, '2000-01-01'::DATE), NULL, TRUE
FROM schema_silver.distributors d
LEFT JOIN schema_gold.dim_distributors dd
    ON d.distributor_id = dd.distributor_id AND dd.is_current = TRUE
WHERE dd.distributor_id IS NULL;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.dim_distributors WHERE is_current = TRUE; -- kỳ vọng = 138
-- SELECT distributor_id, COUNT(*) FROM schema_gold.dim_distributors WHERE is_current = TRUE GROUP BY distributor_id HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng
