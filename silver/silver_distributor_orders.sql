
-- Silver layer: schema_bronze.distributor_orders -> schema_silver.stg_distributor_orders

-- QUAN TRỌNG: 35,945 dòng nhưng order_id chỉ có 8,000 giá trị duy nhất
-- -> mỗi order có NHIỀU DÒNG SẢN PHẨM (order line), order_id KHÔNG PHẢI
-- khoá duy nhất của bảng. Khoá dedup đúng phải là composite:
-- (order_id, product_id) -- giả định 1 sản phẩm chỉ xuất hiện 1 lần/đơn.
-- Nếu sau này phát hiện có đơn hàng lặp lại cùng product_id nhiều dòng
-- (vd 2 lô giao khác ngày), cần bổ sung thêm cột vào PARTITION BY.


DROP TABLE IF EXISTS schema_silver.distributor_orders;

CREATE TABLE schema_silver.distributor_orders AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id), TRIM(product_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.distributor_orders
    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
)
SELECT
    TRIM(order_id)                                                    AS order_id,
    TO_DATE(NULLIF(TRIM(order_date), ''), 'YYYY-MM-DD')               AS order_date,
    NULLIF(TRIM(order_month), '')::INT                                AS order_month,
    NULLIF(TRIM(order_quarter), '')::INT                              AS order_quarter,
    TRIM(distributor_id)                                              AS distributor_id,
    UPPER(TRIM(region))                                               AS region,
    UPPER(TRIM(channel))                                              AS channel,
    TRIM(product_id)                                                  AS product_id,
    UPPER(TRIM(product_category))                                     AS product_category,
    NULLIF(TRIM(qty_ordered), '')::NUMERIC                            AS qty_ordered,
    NULLIF(TRIM(qty_delivered), '')::NUMERIC                          AS qty_delivered,
    NULLIF(TRIM(fill_rate_pct), '')::NUMERIC                          AS fill_rate_pct,
    NULLIF(TRIM(unit_price_list), '')::NUMERIC                        AS unit_price_list,
    NULLIF(TRIM(distributor_price), '')::NUMERIC                      AS distributor_price,
    NULLIF(TRIM(gross_amount), '')::NUMERIC                           AS gross_amount,
    NULLIF(TRIM(delivered_amount), '')::NUMERIC                       AS delivered_amount,
    TO_DATE(NULLIF(TRIM(expected_delivery_date), ''), 'YYYY-MM-DD')   AS expected_delivery_date,
    TO_DATE(NULLIF(TRIM(actual_delivery_date), ''), 'YYYY-MM-DD')     AS actual_delivery_date,
    INITCAP(TRIM(ontime_delivery))                                    AS ontime_delivery,
    INITCAP(TRIM(delivery_status))                                    AS delivery_status,
    UPPER(TRIM(payment_terms))                                        AS payment_terms,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_distributor_orders; -- kỳ vọng gần bằng 35945 (trừ hao nếu có trùng thật)
-- SELECT order_id, product_id, COUNT(*) FROM schema_silver.stg_distributor_orders
--   GROUP BY 1,2 HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng (xác nhận composite key đúng là unique)
-- SELECT COUNT(*) FROM schema_silver.stg_distributor_orders WHERE qty_delivered > qty_ordered; -- bất thường nếu > 0
