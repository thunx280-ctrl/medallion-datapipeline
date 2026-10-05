-- Loại: SCD TYPE 1 (overwrite) — khi thông tin khách hàng thay đổi
-- (vd đổi số điện thoại, đổi địa chỉ), bản ghi cũ bị GHI ĐÈ, KHÔNG giữ
-- lại lịch sử. Phù hợp cho customer vì lịch sử thay đổi thông tin liên
-- hệ không có giá trị phân tích (khác với employee - cần biết nhân viên
-- TỪNG thuộc vùng nào để tính đúng doanh số cho từng thời kỳ).
--
-- customer_sk: SURROGATE KEY (khoá thay thế) - số nguyên tự sinh, dùng
-- để fact_sales/fact_returns JOIN tới dim_customers. Không dùng thẳng
-- customer_id (business key) làm khoá join trong fact, vì:
--   1) customer_sk là INT, join nhanh hơn TEXT
--   2) Tách biệt khoá kỹ thuật (sk) và khoá nghiệp vụ (customer_id) là
--      best practice Star Schema, dù ở SCD1 lợi ích chưa rõ bằng SCD2
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.dim_customers;

CREATE TABLE schema_gold.dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customer_id)   AS customer_sk,  -- surrogate key
    customer_id,                                                -- natural/business key
    customer_name,
    customer_type,
    channel,
    province,
    region,
    address,
    phone,
    tax_code,
    join_date,
    credit_limit,
    status,
    NOW()                                       AS _loaded_at   -- lần Gold này được build
FROM schema_silver.customers;

ALTER TABLE schema_gold.dim_customers ADD PRIMARY KEY (customer_sk);
ALTER TABLE schema_gold.dim_customers ADD CONSTRAINT uq_dim_customers_customer_id UNIQUE (customer_id);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.dim_customers; -- kỳ vọng = 2000 (khớp stg_customers)
-- SELECT COUNT(*) FROM schema_gold.dim_customers WHERE customer_sk IS NULL; -- kỳ vọng = 0
-- SELECT customer_id, COUNT(*) FROM schema_gold.dim_customers GROUP BY customer_id HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng (xác nhận customer_id vẫn unique)
