-- Silver layer: schema_bronze.products -> schema_silver.products

-- Dựa trên kết quả profiling + mẫu dữ liệu thật đã xác nhận:
--   - product_id: 100/100 không trùng -> dùng làm khoá dedup
--   - unit_price, cost_price, weight_gram: số nguyên thuần, không dấu
--     phẩy/đơn vị chữ kèm theo (vd "12000", không phải "12,000" hay "500g")
--   - launch_date: format 'YYYY-MM-DD' (vd 2023-06-09)
--   - Không có cột nào null ở Bronze (theo profiling) -> không cần xử lý
--     null đặc biệt như bảng customers
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.products;

CREATE TABLE schema_silver.products AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(product_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.products
    WHERE product_id IS NOT NULL AND TRIM(product_id) <> ''
)
SELECT
    TRIM(product_id)                   AS product_id,
    TRIM(product_name)                 AS product_name,
    TRIM(category)                     AS category,
    TRIM(sub_category)                 AS sub_category,
    TRIM(unit)                         AS unit,
    NULLIF(TRIM(unit_price), '')::NUMERIC  AS unit_price,
    NULLIF(TRIM(cost_price), '')::NUMERIC  AS cost_price,
    NULLIF(TRIM(weight_gram), '')::NUMERIC AS weight_gram,
    INITCAP(TRIM(status))                  AS status,
    TO_DATE(NULLIF(TRIM(launch_date), ''), 'YYYY-MM-DD') AS launch_date,
    _source_file,
    _source_platform,
    _ingested_at,
    _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT — chạy tay sau khi thực thi file này:
-- SELECT COUNT(*) FROM schema_silver.stg_products;                             -- kỳ vọng = 100
-- SELECT COUNT(*) FROM schema_silver.stg_products WHERE unit_price IS NULL;    -- kỳ vọng = 0
-- SELECT COUNT(*) FROM schema_silver.stg_products WHERE cost_price > unit_price; -- kiểm tra bất thường: giá vốn > giá bán (nếu có, báo lại để xem xét)
-- SELECT category, sub_category, COUNT(*) FROM schema_silver.stg_products GROUP BY 1,2 ORDER BY 1,2;
