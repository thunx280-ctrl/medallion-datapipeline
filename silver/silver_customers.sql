-- Silver layer: schema_bronze.customers -> schema_silver.stg_customers

-- Dựa trên kết quả profiling (Profiling_Report.xlsx):
--   customer_id: 2000/2000 dòng không trùng -> dùng làm khoá dedup
--   tax_code: 51.75% null -> BÌNH THƯỜNG (khách cá nhân không có mã số
--     thuế, chỉ doanh nghiệp mới có) -> GIỮ NGUYÊN NULL, không điền mặc định
--   join_date: xác nhận format 'YYYY-MM-DD' (vd 2023-11-17)
--   credit_limit: xác nhận số nguyên thuần, không dấu phẩy (vd 100000000)

DROP TABLE IF EXISTS schema_silver.customers;
CREATE TABLE schema_silver.customers AS

WITH dedup AS (
    -- Loại trùng lặp theo customer_id: nếu loader chạy lại nhiều lần
    -- (Bronze luôn append), giữ lại bản ghi được nạp GẦN NHẤT (_ingested_at lớn nhất) cho mỗi customer_id.
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(customer_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.customers
    WHERE customer_id IS NOT NULL AND TRIM(customer_id) <> ''
)
SELECT
    TRIM(customer_id)                                          AS customer_id,
    TRIM(customer_name)                                        AS customer_name,
    UPPER(TRIM(customer_type))                                 AS customer_type,
    UPPER(TRIM(channel))                                       AS channel,
    TRIM(province)                                             AS province,
    UPPER(TRIM(region))                                        AS region,
    TRIM(address)                                              AS address,

    -- Chuẩn hoá phone: bỏ khoảng trắng/gạch ngang, chỉ giữ số và dấu "+"
    REGEXP_REPLACE(TRIM(phone), '[^0-9+]', '', 'g')            AS phone,

    -- tax_code: chuỗi rỗng -> NULL thật (KHÔNG dùng giá trị mặc định như 'N/A')
    NULLIF(TRIM(tax_code), '')                                 AS tax_code,
    TO_DATE(NULLIF(TRIM(join_date), ''), 'YYYY-MM-DD')         AS join_date,
    NULLIF(TRIM(credit_limit), '')::NUMERIC                    AS credit_limit,
    INITCAP(TRIM(status))                                      AS status,
    -- Giữ lại metadata để truy vết nguồn gốc dữ liệu khi cần debug
    _source_file,
    _source_platform,
    _ingested_at,
    _batch_id
FROM dedup
WHERE rn = 1;

-- SELECT COUNT(*) FROM schema_silver.stg_customers;                         -- kỳ vọng = 2000 (hoặc ít hơn nếu có trùng)
-- SELECT COUNT(*) FROM schema_silver.stg_customers WHERE customer_id IS NULL; -- kỳ vọng = 0
-- SELECT COUNT(*) FROM schema_silver.stg_customers WHERE join_date IS NULL; -- kiểm tra có row nào cast ngày lỗi không
-- SELECT customer_type, COUNT(*) FROM schema_silver.stg_customers GROUP BY customer_type; -- xem 6 loại customer_type sau chuẩn hoá
