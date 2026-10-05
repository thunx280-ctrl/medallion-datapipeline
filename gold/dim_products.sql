-- Gold layer: schema_silver.stg_products -> schema_gold.dim_products
-- Loại: SCD TYPE 1 (overwrite) — giống dim_customers, thông tin sản phẩm
-- thay đổi (vd đổi giá) sẽ ghi đè, không giữ lịch sử giá cũ ở dimension
-- này (nếu cần phân tích lịch sử giá, đó là việc của fact_sales — vì
-- mỗi dòng fact_sales đã lưu sẵn unit_price TẠI THỜI ĐIỂM bán).

-- product_sk: surrogate key, nhất quán với toàn bộ Gold layer (đã thống
-- nhất áp dụng cho mọi dimension, kể cả SCD1).
-- =====================================================================

DROP TABLE IF EXISTS schema_gold.dim_products;

CREATE TABLE schema_gold.dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY product_id) AS product_sk,
    product_id,
    product_name,
    category,
    sub_category,
    unit,
    unit_price,
    cost_price,
    weight_gram,
    status,
    launch_date,
    NOW()  AS _loaded_at
FROM schema_silver.products;

ALTER TABLE schema_gold.dim_products ADD PRIMARY KEY (product_sk);
ALTER TABLE schema_gold.dim_products ADD CONSTRAINT uq_dim_products_product_id UNIQUE (product_id);

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_gold.dim_products; -- kỳ vọng = 100 (khớp stg_products)
-- SELECT product_id, COUNT(*) FROM schema_gold.dim_products GROUP BY product_id HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng
