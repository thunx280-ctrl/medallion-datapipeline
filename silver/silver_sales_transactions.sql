
-- Silver layer: schema_bronze.sales_transactions -> schema_silver.stg_sales_transactions

-- QUAN TRỌNG: giống distributor_orders — 119,101 dòng nhưng order_id chỉ
-- có 50,000 giá trị duy nhất -> mỗi đơn hàng có NHIỀU DÒNG SẢN PHẨM.
-- Khoá dedup đúng là composite: (order_id, product_id).
-- =====================================================================

DROP TABLE IF EXISTS schema_silver.sales_transactions;

CREATE TABLE schema_silver.sales_transactions AS
WITH dedup AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY TRIM(order_id), TRIM(product_id)
            ORDER BY _ingested_at DESC
        ) AS rn
    FROM schema_bronze.sales_transactions
    WHERE order_id IS NOT NULL AND TRIM(order_id) <> ''
)
SELECT
    TRIM(order_id)                                              AS order_id,
    TO_DATE(NULLIF(TRIM(order_date), ''), 'YYYY-MM-DD')         AS order_date,
    NULLIF(TRIM(order_month), '')::INT                          AS order_month,
    NULLIF(TRIM(order_quarter), '')::INT                        AS order_quarter,
    NULLIF(TRIM(order_year), '')::INT                           AS order_year,
    TRIM(customer_id)                                           AS customer_id,
    UPPER(TRIM(region))                                         AS region,
    TRIM(province)                                              AS province,
    UPPER(TRIM(channel))                                        AS channel,
    TRIM(employee_id)                                           AS employee_id,
    TRIM(product_id)                                            AS product_id,
    UPPER(TRIM(product_category))                               AS product_category,
    NULLIF(TRIM(quantity), '')::NUMERIC                         AS quantity,
    NULLIF(TRIM(unit_price), '')::NUMERIC                       AS unit_price,
    NULLIF(TRIM(discount_pct), '')::NUMERIC                     AS discount_pct,
    NULLIF(TRIM(discount_amount), '')::NUMERIC                  AS discount_amount,
    NULLIF(TRIM(gross_amount), '')::NUMERIC                     AS gross_amount,
    NULLIF(TRIM(net_amount), '')::NUMERIC                       AS net_amount,
    INITCAP(TRIM(delivery_status))                              AS delivery_status,
    UPPER(TRIM(payment_method))                                 AS payment_method,
    INITCAP(TRIM(payment_status))                               AS payment_status,
    _source_file, _source_platform, _ingested_at, _batch_id
FROM dedup
WHERE rn = 1;

-- CHECKPOINT:
-- SELECT COUNT(*) FROM schema_silver.stg_sales_transactions; -- kỳ vọng gần bằng 119101
-- SELECT order_id, product_id, COUNT(*) FROM schema_silver.stg_sales_transactions
--   GROUP BY 1,2 HAVING COUNT(*) > 1; -- kỳ vọng 0 dòng
-- SELECT COUNT(*) FROM schema_silver.stg_sales_transactions WHERE net_amount > gross_amount; -- bất thường nếu > 0
