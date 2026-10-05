
-- Silver layer: schema_bronze.promotion_program -> schema_silver.promotion_program

-- promotion_id: 40/40 không trùng -> khoá dedup.
-- ĐÃ XÁC NHẬN: applicable_products là danh sách nhiều product_id gộp
-- trong 1 ô, ngăn cách bởi dấu "|" (vd "P001|P002|P015"). Bảng chính
-- dưới đây vẫn giữ nguyên dạng text gốc (để truy vết); bên dưới có thêm
-- bảng schema_silver.stg_promotion_products đã TÁCH SẴN thành quan hệ
-- nhiều-nhiều chuẩn (1 dòng = 1 cặp promotion_id - product_id), để Gold
-- layer join thẳng, không cần xử lý chuỗi nữa.
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.promotion_program;

CREATE TABLE schema_silver.promotion_program AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(promotion_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.promotion_program
    WHERE promotion_id IS NOT NULL AND TRIM(promotion_id) <> ''
)
SELECT
    TRIM(promotion_id)                                          AS promotion_id,
    TRIM(promotion_name)                                        AS promotion_name,
    UPPER(TRIM(promotion_type))                                 AS promotion_type,
    UPPER(TRIM(target_channel))                                 AS target_channel,
    UPPER(TRIM(target_region))                                  AS target_region,
    TO_DATE(NULLIF(TRIM(start_date), ''), 'YYYY-MM-DD')         AS start_date,
    TO_DATE(NULLIF(TRIM(end_date), ''), 'YYYY-MM-DD')           AS end_date,
    TRIM(applicable_products)                                   AS applicable_products,  -- xem ghi chú trên: có thể là danh sách
    NULLIF(TRIM(discount_pct), '')::NUMERIC                     AS discount_pct,
    NULLIF(TRIM(min_order_quantity), '')::NUMERIC               AS min_order_quantity,
    NULLIF(TRIM(budget_vnd), '')::NUMERIC                       AS budget_vnd,
    NULLIF(TRIM(actual_cost_vnd), '')::NUMERIC                  AS actual_cost_vnd,
    INITCAP(TRIM(status))                                       AS status,
    TRIM(created_by)                                            AS created_by,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_promotion_program; -- kỳ vọng = 40
-- SELECT COUNT(*) FROM schema_silver.stg_promotion_program WHERE actual_cost_vnd > budget_vnd; -- bất thường nếu chi vượt ngân sách

-- =====================================================================
-- Bảng phụ: tách applicable_products (ngăn cách bởi "|") thành quan hệ
-- nhiều-nhiều chuẩn: 1 dòng = 1 cặp (promotion_id, product_id).
--
-- STRING_TO_ARRAY(..., '|')  -> tách chuỗi "P001|P002|P015" thành mảng
--                                ['P001', 'P002', 'P015']
-- UNNEST(...)                -> "bung" mảng đó thành nhiều DÒNG riêng
--                                (1 mảng 3 phần tử -> 3 dòng)
-- TRIM(...)                  -> phòng trường hợp có khoảng trắng thừa
--                                quanh dấu "|" (vd "P001 | P002")
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.promotion_products;

CREATE TABLE schema_silver.promotion_products AS
SELECT
    promotion_id,
    TRIM(UNNEST(STRING_TO_ARRAY(applicable_products, '|'))) AS product_id
FROM schema_silver.promotion_program
WHERE applicable_products IS NOT NULL AND TRIM(applicable_products) <> '';

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_promotion_products; -- kỳ vọng > 40 (vì mỗi promotion có nhiều product)
-- SELECT promotion_id, COUNT(*) AS so_product
--   FROM schema_silver.stg_promotion_products
--   GROUP BY promotion_id ORDER BY so_product DESC LIMIT 5; -- xem promotion nào áp dụng nhiều sản phẩm nhất
-- -- Kiểm tra product_id tách ra có tồn tại thật trong stg_products không (phát hiện lỗi nhập liệu id sai):
-- SELECT DISTINCT pp.product_id
-- FROM schema_silver.stg_promotion_products pp
-- LEFT JOIN schema_silver.stg_products p ON pp.product_id = p.product_id
-- WHERE p.product_id IS NULL;
